
from datetime import datetime
from random import randint
import time
import calendar
from pprint import pformat
from json import loads as json_loads
import os.path
from cStringIO import StringIO
from cPickle import dumps as pickle_dumps
from hashlib import sha1

#from bson import json_util, BSON
from bson.json_util import object_hook
import pymongo
import monkeymongo #monkey patch pymongo to automatically reconnect
from flask import Flask, request
app = Flask(__name__)
app.config.from_object('clio.settings')
try:
    app.config.from_envvar('CLIO_SETTINGS')
except RuntimeError:
    if os.path.exists('/etc/clio/app.conf'):
        app.logger.debug("couldn't load settings from file in envvar. trying /etc/clio/app.conf")
        try:
            app.config.from_pyfile('/etc/clio/app.conf')
        except RuntimeError:
            app.logger.debug("unable to find any settings files. using defaults in local settings module.")
    else:
        app.logger.debug("unable to find any settings files. using defaults in local settings module.")

from utils import ExtRequest
app.request_class = ExtRequest

mongo_conn = None
def get_mongo_conn():
    global mongo_conn
    if mongo_conn is None:
        mongo_conn = pymongo.Connection(app.config['MONGO_HOSTS'], app.config['MONGO_PORT']).clio
    return mongo_conn


def validify_data(data):
    if not isinstance(data, dict):
        return data

    for key in data.iterkeys():
        if isinstance(data[key], dict):
            data[key] = validify_data(data[key])
        if '.' in key:
            data[key.replace('.', '__DOT__')] = data.pop(key)
    return data

from pyes import ES as _ES
from pyes.exceptions import ElasticSearchException

class ES(_ES):
    def force_bulk(self):
        """
        Force executing of all bulk data
        """
        if self.bulk_items:
            bulk_data = self.bulk_data
            self.bulk_data = StringIO()
            self.bulk_items = 0
            return self._send_request("POST", "/_bulk", bulk_data.getvalue())


@app.route("/batch_store", methods=['PUT', 'POST'])
def batch():

    conn = ES('%s:%s' % (app.config['ES_HOST'], app.config['ES_PORT']), timeout=30)

    spool = StringIO(request.data)

    record = spool.read(int(spool.readline()))
    host,sourcetype,extra = json_loads(record, object_hook=object_hook)

    app.logger.info("host: %s, sourcetype: %s" % (host, sourcetype))

    index_name = 'clio_%s' % extra['started_timestamp'].strftime('%Y%m')

    def _iter_records(spool, validify=False):
        while 1:
            lenprefix = spool.readline()
            if not lenprefix:
                break
            lenprefix = int(lenprefix)
            record = spool.read(lenprefix)

            if len(record) != lenprefix:
                app.logger.error("Malformed record!")

            timestamp, data = json_loads(record, object_hook=object_hook)
            if validify:
                yield timestamp, validify_data(data)
            else:
                yield timestamp, data




    if extra.get('timestamp_as_id', False):
        merged = {}
        for timestamp,data in _iter_records(spool):




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


            #app.logger.debug("result: %s" % pformat(result))
            assert result['ok']




    else:

        def _schema(timestamp, data):
            if extra.get('custom_schema', False):
                doc = data
            else:
                doc = {'timestamp': timestamp,
                       'host': host,
                       'data': data}
            return doc

        multi = extra.get('multi', False)

        for timestamp,data in _iter_records(spool):

            recordid = [int(calendar.timegm( timestamp.timetuple() )), host]
            if multi:
                if multi in data:
                    key_part =  data[multi]
                else:
                    key_part = sha1( pickle_dumps(data) ).hexdigest()
                recordid.append( key_part )
            recordid = recordid.join(':')
            try:
                conn.index(_schema(timestamp,data), index_name, sourcetype, id=recordid, bulk=True)
            except Exception, e:
                app.logger.exception("record id: %s, data: %s" % (recordid, pformat(data)))
                raise e

        result = conn.force_bulk()
        if result:
            for status in result['items']:
                assert status['create']['ok']

    return "ok"




@app.route("/store/<host>/<sourcetype>/<float:timestamp>", methods=['PUT', 'POST'])
def store(host, sourcetype, timestamp):

    timestamp = datetime.utcfromtimestamp(timestamp)

    if request.json is not None:
        data = request.json
    else:
        app.logger.debug("data was in raw format")
        data = request.data

    app.logger.info("host: %s, sourcetype: %s, timestamp: %s" % (host, sourcetype, timestamp))

    db = get_mongo_conn()
    coll = db['%s_%s' % (sourcetype, timestamp.strftime('%Y%m'))]

    extra = request.headers.get('extra', {})
    if extra:
        extra = json_loads(extra, object_hook=object_hook)

    if extra.get('timestamp_as_id', False):
        spec = {'_id': timestamp}
        index = [('_id', pymongo.DESCENDING)]
        coll.ensure_index(index, background=True)
    else:
        spec = {'ts': timestamp,
                'host': host}
        index = [('host', pymongo.ASCENDING),
                 ('ts', pymongo.DESCENDING)]
        coll.ensure_index(index, background=True, unique=True)

    if extra.get('custom_schema', False):
        doc = data
    else:
        doc = {'ts': timestamp,
               'host': host,
               'data': data}

    coll.update(spec, doc, upsert=True, safe=True)

    return "ok"


@app.route("/ping")
def ping():
    return "pong"


if __name__ == '__main__':
    app.run(host=app.config['HOST'], port=app.config['PORT'])

