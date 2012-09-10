

try:
    import xtraceback
    xtraceback.compat.install()
except ImportError:
    pass

from datetime import date, datetime
from json import dump, load
from uuid import uuid4
from pprint import pformat
from cStringIO import StringIO
import signal
import sys
import logging

logger = logging.getLogger('clio')

from gevent import spawn, sleep, joinall, killall
from gevent.event import Event
from gevent.coros import RLock
import zmq.green as zmq
from geventhttpclient import httplib

from clio.process import process

BULKTHRESHOLD = 2*1024*1024

ERROR = '\xff'
SUCCESS = '\x00'
NOTDONE = '\x01'



class record_spool(object):
    def __init__(self, urn):
        self.urn = urn
        self.records = {}

    def __nonzero__(self):
        'True if done'
        return not self.records


def json_encode_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, 0, 0, 0)
        return dt.isoformat()
    return value




class indexer(object):
    def __init__(self, registry, host='localhost', port=9200, timeout=300):
        self.registry = registry
        self.host = host
        self.port = port
        self.timeout = timeout

    def get_conn(self):
        return httplib.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def run(self):
        es = self.get_conn()
        registry = self.registry
        logger.info("starting indexer")

        while 1:
            registry.bulk_rest.wait(5)
            registry.bulk_rest.clear()
            bulk_data = registry.flush_bulk()
            if bulk_data is None:
                logger.info("queue empty")
                continue
            elif not isinstance(bulk_data, basestring):
                bulk_data = bulk_data.getvalue()

            logger.info("starting persist run")
            registry.bulk_run.set()
            while 1:
                try:
                    es.request('POST', '/_bulk', body=bulk_data)
                    bulkresult = es.getresponse()
                except httplib.CannotSendRequest:
                    logger.warn("got a cannotsendrequest error from httplib. throwing that instance away and getting a new one.")
                    es = self.get_conn()
                    continue
                except Exception:
                    logger.exception('halp!')
                    sleep(5)
                    continue
                #TODO: timeout
                if bulkresult.status in (200,201,202):
                    self.verify(bulkresult)
                    registry.bulk_run.clear()
                    break
                logger.error(bulkresult.read())
                sleep(5)


    def verify(self, bulkresult):
        if not bulkresult:
            return None

        bulkresult = load(bulkresult)
        logger.info("verifying individual results from bulk request")
        for status in bulkresult['items']:
            for result in status.itervalues():
                recordid = result['_id']
                if result['ok']:
                    self.registry.completed_record(recordid)
                else:
                    #TODO: make warning more helpful
                    logger.warn(pformat(result))
                    self.registry.failed_record(recordid)

        sleep(0)



class registry(object):
    def __init__(self, bulk_threshold=BULKTHRESHOLD):
        self.bulk_threshold = bulk_threshold
        self.record_registry = {}
        self.receipt_registry = {}
        self.pause_indexing = Event()
        self.pause_indexing.set()
        self.queue_not_full = Event()
        self.queue_not_full.set()
        self.bulk_run = Event()
        self.bulk_rest = Event()
        self._bulk_lock = RLock()
        self.bulk_data = StringIO()
        signal.signal(signal.SIGUSR1, self.toggle_pause)

    def toggle_pause(self, *args, **kwargs):
        if self.pause_indexing.is_set():
            return self.pause_indexing.clear()
        return self.pause_indexing.set()

    def queue_ready(self):
        logger.info("making sure the queue is ready for us to put stuff in it")
        deadlock = False
        while 1:
            self.pause_indexing.wait()
            ready = self.queue_not_full.wait(60)
            if ready:
                logger.info("ready to start filling up queue")
                return
            elif not self.bulk_run.is_set():
                if not deadlock:
                    deadlock = True
                else:
                    logger.error("deadlock detected! going to set queue_not_full event")
                    self.queue_not_full.set()
                    return
            logger.info("waiting on persist client to finish so next batch can start")

    def add_bulk(self, recordid, (doc, index_name, sourcetype)):
        logger.info("adding record %s to bulk queue" % recordid)
        header = dict(index=dict(_index=index_name, _type=sourcetype, _id=recordid))
        with self._bulk_lock:
            for data in (header, doc):
                dump(data, self.bulk_data, default=json_encode_default)
                self.bulk_data.write('\n')

            if self.bulk_data.tell() >= self.bulk_threshold:
                logger.info("queue has surpassed threshold")
                #start the bulk persist early
                if self.bulk_run.is_set():
                    # if the persist client is in the middle of a trip and the
                    # queue is full, clear the queue_not_full event so the
                    # persister waits
                    logger.info("persist client running, and queue is full. clear queue_not_full flag")
                    self.queue_not_full.clear()

            self.bulk_rest.set()


    def flush_bulk(self):
        self.pause_indexing.wait()
        if not self.bulk_data.tell():
            return None

        with self._bulk_lock:
            bulk_data = self.bulk_data
            self.bulk_data = StringIO()

            self.queue_not_full.set()
        logger.info("bulk queue flushed, so there is definitely room in the queue. set the flag")
        return bulk_data

    def register_spool(self, receipt, records):
        logger.info("registering spool of records for receipt %s" % receipt)
        add_bulk = self.add_bulk
        update_record_registry = self.record_registry.update

        rspool = record_spool(receipt)
        self.receipt_registry[receipt] = rspool
        for recordid,record in records:
            update_record_registry( ((recordid,rspool),) )
            rspool.records[recordid] = record
            add_bulk(recordid, record)

    def check_spool_status(self, receipt):
        logger.info("checking the status of the records from spool for receipt: %s" % receipt)
        if receipt in self.receipt_registry:
            return bool( self.receipt_registry[receipt] )

    def failed_record(self, recordid):
        logger.info("record failed insertion in index: %s" % recordid)
        #TODO: add registry of number of failed attempts for a record
        spool_obj = self.record_registry[recordid]
        record_obj = spool_obj.records[recordid]
        self.add_bulk(recordid, record_obj)

    def completed_record(self, recordid):
        logger.info("record successfully persisted: %s" % recordid)
        spool_obj = self.record_registry.pop(recordid)
        del spool_obj.records[recordid]


class receiver(object):
    def __init__(self, registry, address):
        self.registry = registry
        self.address = address

    def main(self, socket):
        while 1:
            action = socket.recv()
            dict(persist=self.handle_persist,
                 check=self.handle_check)[action](socket)
            sleep(0)


    def handle_persist(self, socket):
        self.registry.queue_ready()
        receipt = uuid4().urn
        data = socket.recv(copy=False)
        self.registry.register_spool(receipt, process(data))
        socket.send_multipart((self.address, receipt))
        #socket.send_pyobj((self.address, receipt))
        #TODO: send receipt back to client with our 'address'

    def handle_check(self, socket):
        receipt = socket.recv()
        status = self.registry.check_spool_status(receipt)

        if status:
            del self.registry.receipt_registry[receipt]

        socket.send({
            True: SUCCESS,
            False: NOTDONE,
            None: ERROR
        }[status])




if __name__ == '__main__':

    logger.setLevel(logging.DEBUG)
    loggerHandler = logging.StreamHandler(sys.stdout)
    loggerHandler.setLevel(logging.DEBUG)
    loggerFormatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    loggerHandler.setFormatter(loggerFormatter)
    logger.addHandler(loggerHandler)


    try:
        host = sys.argv[1]
    except:
        host = '127.0.0.1'
    try:
        port = int(sys.argv[2])
    except:
        port = 9200
    threads = []
    spool_register = registry()
    the_indexer = indexer(spool_register, host=host, port=port)
    the_receiver = receiver(spool_register, 'clio')


    context = zmq.Context()
    sock = context.socket(zmq.REP)
    sock.setsockopt(zmq.IDENTITY, 'clio')
    sock.bind('tcp://0.0.0.0:64000')

    #from gevent.backdoor import BackdoorServer
    #b = BackdoorServer(('127.0.0.1', 60000), locals=dict(spool_register=spool_register, the_indexer=the_indexer, the_receiver=the_receiver))
    #threads.append(spawn(b.serve_forever))

    threads.append(spawn(the_indexer.run))
    threads.append(spawn(the_receiver.main, sock))

    def die_in_death(_):
        killall(threads)

    [_.link_exception(die_in_death) for _ in threads]
    try:
        joinall(threads)
    except KeyboardInterrupt:
        pass



