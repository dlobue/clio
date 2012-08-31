


from random import randint
import time
import calendar
from pprint import pformat
from json import loads as json_loads
from cStringIO import StringIO
from cPickle import dumps as pickle_dumps
from hashlib import sha1
import sys

import logging

logger = logging.getLogger('clio')





#from pyes import ES
#from pyes.exceptions import ElasticSearchException

#from bson import json_util, BSON
from bson.json_util import object_hook


def validify_data(data):
    if not isinstance(data, dict):
        return data

    for key in data.iterkeys():
        if isinstance(data[key], dict):
            data[key] = validify_data(data[key])
        if '.' in key:
            data[key.replace('.', '__DOT__')] = data.pop(key)
    return data



def _iter_records(spool, validify=False):
    while 1:
        lenprefix = spool.readline()
        if not lenprefix:
            break
        lenprefix = int(lenprefix)
        record = spool.read(lenprefix)

        if len(record) != lenprefix:
            logger.error("Malformed record!")

        timestamp, data = json_loads(record, object_hook=object_hook)
        if validify:
            yield timestamp, validify_data(data)
        else:
            yield timestamp, data



def process_records(sourcetype, index_name, extra, host, records):

        def _schema(timestamp, data):
            if extra.get('custom_schema', False):
                doc = data
            else:
                doc = {'timestamp': timestamp,
                       'host': host,
                       'data': data}
            return doc

        multi = extra.get('multi', False)

        for timestamp,data in records:

            recordid = [int(calendar.timegm( timestamp.timetuple() )), host]
            if multi:
                if multi in data:
                    key_part =  data[multi]
                else:
                    key_part = sha1( pickle_dumps(data) ).hexdigest()
                recordid.append( key_part )
            recordid = ':'.join(map(str, recordid))

            yield (recordid, (_schema(timestamp,data), index_name, sourcetype))


def process_standard_records(conn, sourcetype, index_name, extra, host, records):

        def _schema(timestamp, data):
            if extra.get('custom_schema', False):
                doc = data
            else:
                doc = {'timestamp': timestamp,
                       'host': host,
                       'data': data}
            return doc

        multi = extra.get('multi', False)

        for timestamp,data in records:

            recordid = [int(calendar.timegm( timestamp.timetuple() )), host]
            if multi:
                if multi in data:
                    key_part =  data[multi]
                else:
                    key_part = sha1( pickle_dumps(data) ).hexdigest()
                recordid.append( key_part )
            recordid = ':'.join(map(str, recordid))
            try:
                conn.index(_schema(timestamp,data), index_name, sourcetype, id=recordid, bulk=True)
            except Exception, e:
                logger.exception("record id: %s, data: %s" % (recordid, pformat(data)))
                raise e


def reformat_records(records):
    def _reformat(data):
        del data['from']
        return data

    for timestamp,data in records:
        if isinstance(data, list):
            for datum in data:
                yield timestamp, _reformat(datum)
            continue
        yield timestamp, _reformat(data)

def process_timestamp_as_id(conn, sourcetype, index_name, records):
    merged = {}
    for timestamp,data in records:

        if isinstance(data, dict):
            data = [data]

        results = merged.setdefault(timestamp, [])
        results.extend(data)


    for timestamp,data in merged.iteritems():
        timestamp = calendar.timegm( timestamp.timetuple() )
        data = dict(data=data)
        doc = dict(script="ctx._source.data += ($ in data if !ctx._source.data.contains($))", params=data)

        path = conn._make_path([index_name, sourcetype, int(timestamp), '_update'])
        c = 0
        MAX_RETRY = 10
        while 1:
            try:
                result = conn._send_request('POST', path, doc, {})
            except ElasticSearchException, e:
                if ((e.status == 404 and e.message.startswith(u'DocumentMissingException'))
                    or e.message == u"Unknown exception type"):
                    try:
                        result = conn.index(data, index_name, sourcetype, id=int(timestamp), querystring_args=dict(op_type='create'))
                        #XXX: may need to use querystring_args
                        #querystring_args=dict(op_type='create')
                    except ElasticSearchException, e:
                        if ((e.status == 409 and e.message.startswith(u'DocumentAlreadyExistsException'))
                            or e.message.startswith(u'VersionConflictEngineException')):
                            c += 1
                            continue
                        else:
                            app.logger.exception("data: %s" % pformat(data))
                            raise e
                elif e.status == 409 and e.message.startswith(u'VersionConflictEngineException'):
                    if c > MAX_RETRY:
                        app.logger.error("passed max retry! returning error!")
                        raise e
                    c += 1
                    time.sleep(randint(1,9)/10.0)
                    continue
                else:
                    app.logger.exception("doc: %s" % pformat(doc))
                    raise e
            break

        assert result['ok']


def process(data):

    spool = StringIO(data)

    record = spool.read(int(spool.readline()))
    host,sourcetype,extra = json_loads(record, object_hook=object_hook)

    logger.info("host: %s, sourcetype: %s" % (host, sourcetype))

    index_name = 'clio_%s' % extra['started_timestamp'].strftime('%Y%m')

    if extra.get('timestamp_as_id', False):
        extra['multi'] = True
        records = reformat_records(_iter_records(spool))
    else:
        records = _iter_records(spool)
    return process_records(sourcetype, index_name, extra, host, records)


def _process(conn, data):

    spool = StringIO(data)

    record = spool.read(int(spool.readline()))
    host,sourcetype,extra = json_loads(record, object_hook=object_hook)

    logger.info("host: %s, sourcetype: %s" % (host, sourcetype))

    index_name = 'clio_%s' % extra['started_timestamp'].strftime('%Y%m')
    #db = get_mongo_conn()
    #coll = db['%s_%s' % (sourcetype, extra['started_timestamp'].strftime('%Y%m'))]

    if extra.get('timestamp_as_id', False):
        extra['multi'] = True
        records = reformat_records(_iter_records(spool))
        #process_timestamp_as_id(conn, sourcetype, index_name, _iter_records(spool))
    else:
        records = _iter_records(spool)
    process_standard_records(conn, sourcetype, index_name, extra, host, records)



class record_spool(object):
    def __init__(self, urn, records):
        self.urn = urn
        if not isinstance(records, dict):
            records = dict(records)
        self.records = records

    def __nonzero__(self):
        'True if done'
        return not self.records

from gevent import spawn, sleep, joinall, monkey
monkey.patch_all()
import zmq.green as zmq
from uuid import uuid4
from collections import deque
import httplib



class indexer(object):
    def __init__(self, registry, host='localhost', port=9200, timeout=300):
        self.registry = registry
        self.host = host
        self.port = port
        self.timeout = timeout

    def run(self):
        es = httplib.HTTPConnection(self.host, self.port, timeout=self.timeout)
        registry = self.registry
        logger.info("starting indexer")

        while 1:
            registry.bulk_rest.wait(5)
            registry.bulk_rest.clear()
            bulk_ids, bulk_data = registry.flush_bulk()
            if bulk_data is None:
                logger.info("queue empty")
                continue
            logger.info("starting persist run")
            registry.bulk_run.set()
            while 1:
                bulk_data.seek(0)
                try:
                    es.request('POST', '/_bulk', body=bulk_data)
                    bulkresult = es.getresponse()
                except Exception:
                    logger.exception('halp!')
                    sleep(5)
                    continue
                #TODO: timeout
                if bulkresult.status in (200,201,202):
                    self.verify(bulkresult.read())
                    registry.bulk_run.clear()
                    break
                logger.error(bulkresult.read())
                sleep(5)


    def verify(self, bulkresult):
        if not bulkresult:
            return None

        if isinstance(bulkresult, basestring):
            bulkresult = json_loads(bulkresult)
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



from datetime import date, datetime
from json import dump
from threading import Lock, Event

class registry(object):
    def __init__(self, bulk_threshold=10*1024*1024):
        self.bulk_threshold = bulk_threshold
        self.record_registry = {}
        self.receipt_registry = {}
        self.queue_not_full = Event()
        self.queue_not_full.set()
        self.bulk_run = Event()
        self.bulk_rest = Event()
        self._bulk_lock = Lock()
        self.bulk_ids = deque()
        self.bulk_data = StringIO()

    def queue_ready(self):
        deadlock = False
        while 1:
            ready = self.queue_not_full.wait(60)
            if ready:
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
            self.bulk_ids.append(recordid)
            for data in (header, doc):
                dump(data, self.bulk_data, default=json_encode_default)
                self.bulk_data.write('\n')

        if self.bulk_data.tell() >= self.bulk_threshold:
            #start the bulk persist early
            if self.bulk_run.is_set():
                # if the persist client is in the middle of a trip and the
                # queue is full, clear the queue_not_full event so the
                # persister waits
                self.queue_not_full.clear()

            self.bulk_rest.set()


    def flush_bulk(self):
        if not self.bulk_ids:
            return None, None

        with self._bulk_lock:
            bulk_data = self.bulk_data
            record_ids = self.bulk_ids
            self.bulk_ids = deque()
            self.bulk_data = StringIO()
        self.queue_not_full.set()
        return record_ids, bulk_data

    def register_spool(self, receipt, records):
        logger.info("registering spool of records for receipt %s" % receipt)
        add_bulk = self.add_bulk
        def _insert(item):
            add_bulk(*item)
            return item
        records = (_insert(_) for _ in records)
        rspool = record_spool(receipt, dict(records))
        self.receipt_registry[receipt] = rspool
        self.record_registry.update( ((_, rspool) for _ in rspool.records.iterkeys()) )

    def check_spool_status(self, receipt):
        logger.info("checking the status of the records from spool for receipt: %s" % receipt)
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
            self.registry.queue_ready()
            message = socket.recv_json()
            dict(persist=self.handle_persist,
                 check=self.handle_check)[message['action']](socket, message)
            sleep(0)


    def handle_persist(self, socket, message):
        receipt = uuid4().urn
        self.registry.register_spool(receipt, process(message['data']))
        socket.send(receipt)
        #socket.send_pyobj((self.address, receipt))
        #TODO: send receipt back to client with our 'address'

    def handle_check(self, socket, message):
        try:
            status = self.registry.check_spool_status(message['data'])
        except KeyError:
            socket.send_json(dict(status=None))
            return

        if status:
            del self.registry.receipt_registry[message['data']]

        socket.send_json(dict(status=status))








def json_encode_default(self, value):

    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, 0, 0, 0)
        return dt.isoformat()
    return value

from gevent.backdoor import BackdoorServer

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
        host = 'localhost'
    try:
        port = int(sys.argv[2])
    except:
        port = 9200
    threads = []
    spool_register = registry()
    the_indexer = indexer(spool_register, host=host, port=port)
    the_receiver = receiver(spool_register, 'something')


    context = zmq.Context()
    sock = context.socket(zmq.REP)
    sock.bind('tcp://0.0.0.0:64000')

    #b = BackdoorServer(('127.0.0.1', 60000), locals=dict(spool_register=spool_register, the_indexer=the_indexer, the_receiver=the_receiver))
    #threads.append(spawn(b.serve_forever))

    threads.append(spawn(the_indexer.run))
    threads.append(spawn(the_receiver.main, sock))

    joinall(threads)


