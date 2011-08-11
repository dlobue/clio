
from datetime import datetime
import json
from json import loads as json_loads
import os.path
from cStringIO import StringIO

#from bson import json_util, BSON
from bson.json_util import object_hook
import pymongo
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



@app.route("/batch_store", methods=['PUT', 'POST'])
def batch():
    spool = StringIO(request.data)

    record = spool.read(int(spool.readline()))
    host,sourcetype,extra = json_loads(record, object_hook=object_hook)

    app.logger.info("host: %s, sourcetype: %s" % (host, sourcetype))

    db = get_mongo_conn()
    coll = db['%s_%s' % (sourcetype, extra['started_timestamp'].strftime('%Y%m'))]

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
        index = [('_id', pymongo.DESCENDING)]

        for timestamp,data in _iter_records(spool):
            spec = {'_id': timestamp}

            if hasattr(data, '__iter__') and not hasattr(data, 'setdefault'):
                data = { '$each': data }
            data = {'$addToSet':
                    {'data': data }
                   }


            coll.ensure_index(index, background=True)
            coll.update(spec, data, upsert=True)


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

        coll.ensure_index(index, background=True, unique=True)
        coll.insert((_schema(ts,data) for ts,data in _iter_records(spool, True)))

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

