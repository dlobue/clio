


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

logger = logging.getLogger('clio.pykka')



import gevent
import zerorpc
from pykka.gevent import GeventActor
from pykka.registry import ActorRegistry


from pyes import ES
from pyes.exceptions import ElasticSearchException

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



def process(conn, data):

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



from gevent import spawn
import zmq.green as zmq

def handle(socket):
    es = ES('%s:%s' % ('userver03', '9200'), timeout=305)
    while 1:
        message = socket.recv()
        process(es, message)

        result = es.flush_bulk()
        if result:
            for status in result['items']:
                if 'create' in status:
                    assert status['create']['ok'], pformat(status)
                elif 'index' in status:
                    assert status['index']['ok'], pformat(status)



class ESActor(GeventActor):
    def __init__(self):
        self.es = ES('%s:%s' % ('userver03', '9200'), timeout=305)

    def queue_data(self, data):
        process(self.es, data)

        result = self.es.flush_bulk()
        if result:
            for status in result['items']:
                if 'create' in status:
                    assert status['create']['ok'], pformat(status)
                elif 'index' in status:
                    assert status['index']['ok'], pformat(status)


if __name__ == '__main__':

    logger.setLevel(logging.DEBUG)
    loggerHandler = logging.StreamHandler(sys.stdout)
    loggerHandler.setLevel(logging.DEBUG)
    loggerFormatter = logging.Formatter('%(asctime)s [%(name)s] [%(funcName)s] [%(thread)d] %(levelname)s: %(message)s')
    loggerHandler.setFormatter(loggerFormatter)
    logger.addHandler(loggerHandler)

    s = zerorpc.Server(ESActor.start())
    s.bind("tcp://0.0.0.0:4242")
    try:
        s.run()
    except KeyboardInterrupt:
        ActorRegistry.stop_all()





