
from datetime import datetime
import time
import calendar
import json
from json import loads as json_loads
import os.path
from cStringIO import StringIO

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
            r = self._send_request("POST", "/_bulk", self.bulk_data.getvalue())
            self.bulk_data = StringIO()
            self.bulk_items = 0
            return r

conn = ES('userver02:9200')


@app.route("/batch_store", methods=['PUT', 'POST'])
def batch():
    spool = StringIO(request.data)

    record = spool.read(int(spool.readline()))
    host,sourcetype,extra = json_loads(record, object_hook=object_hook)

    app.logger.info("host: %s, sourcetype: %s" % (host, sourcetype))

    #db = get_mongo_conn()
    #coll = db['%s_%s' % (sourcetype, extra['started_timestamp'].strftime('%Y%m'))]

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



    from pprint import pformat

    if extra.get('timestamp_as_id', False):
        index = [('_id', pymongo.DESCENDING)]

        for timestamp,data in _iter_records(spool):
            #spec = {'_id': timestamp}

            #if hasattr(data, '__iter__') and not hasattr(data, 'setdefault'):
                #data = { '$each': data }
            #data = {'$addToSet':
                    #{'data': data }
                   #}


            #coll.ensure_index(index, background=True)
            #coll.update(spec, data, upsert=True)
            #TODO

            if isinstance(data, dict):
                data = [data]
            timestamp = calendar.timegm( timestamp.timetuple() )
            data = dict(data=data)
            doc = dict(script="ctx._source.data += ($ in data if !ctx._source.data.contains($))", params=data)
            #doc = dict(script="ctx._source.data += data", params=data)
            app.logger.debug("data: %s" % pformat(data))

            path = conn._make_path(['clio', sourcetype, int(timestamp), '_update'])
            try:
                result = conn._send_request('POST', path, doc, {})
            except ElasticSearchException, e:
                if e.status == 404 and e.message.startswith(u'DocumentMissingException'):
                    try:
                        result = conn.index(data, 'clio', sourcetype, id=int(timestamp), querystring_args=dict(op_type='create'))
                        #result = conn.index(dict(data=data), 'clio', sourcetype, id=int(timestamp), op_type='create')
                        #XXX: may need to use querystring_args
                        #querystring_args=dict(op_type='create')
                    except ElasticSearchException, e:
                        if e.status == 409 and e.message.startswith(u'DocumentAlreadyExistsException'):
                            result = conn._send_request('POST', path, doc, {})
                        else:
                            raise e
                else:
                    raise e


            #TODO: ensure ok: true
            app.logger.debug("result: %s" % pformat(result))
            assert result['ok']


    else:

        index = [('host', pymongo.ASCENDING),
                 ('ts', pymongo.DESCENDING)]

        def _schema(timestamp, data):
            if extra.get('custom_schema', False):
                doc = data
            else:
                doc = {'ts': timestamp,
                       'host': host,
                       'data': data}
            return doc

        #coll.ensure_index(index, background=True, unique=True)
        #coll.insert((_schema(ts,data) for ts,data in _iter_records(spool, True)))

        for ts,data in _iter_records(spool, True):
            app.logger.debug("data: %s" % pformat(data))
            result = conn.index(_schema(ts,data), 'clio', sourcetype, bulk=True)
            #app.logger.info("result: %s" % pformat(result))

        result = conn.force_bulk()
        for status in result['items']:
            assert status['create']['ok']
        #TODO: ensure ok: true
        app.logger.debug("force_bulk result: %s" % pformat(result))

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

