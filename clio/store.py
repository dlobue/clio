
from datetime import datetime
import os.path

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

import gevent
import zerorpc
from pykka.gevent import GeventActor
from pykka.registry import ActorRegistry
from pykka.proxy import ActorProxy


_esloader = zerorpc.Client(connect_to='tcp://localhost:4242')
esloader = ActorProxy(_esloader)


@app.route("/batch_store", methods=['PUT', 'POST'])
def batch():
    r = esloader.queue_data(request.data)
    if hasattr(r, 'get'):
        r.get()
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

