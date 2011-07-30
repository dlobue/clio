
from datetime import datetime
import json
import os.path

from bson import json_util
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


@app.route("/store/<host>/<sourcetype>/<float:timestamp>", methods=['PUT'])
def store(host, sourcetype, timestamp):

    timestamp = datetime.utcfromtimestamp(timestamp)

    if request.json is not None:
        data = request.json
    else:
        app.logger.debug("data was in raw format")
        data = request.data

    app.logger.info("host: %s, sourcetype: %s, timestamp: %s" % (host, sourcetype, timestamp))

    extra = request.headers.get('extra', {})
    if extra:
        extra = json.loads(extra, object_hook=json_util.object_hook)

    if extra.get('timestamp_as_id', False):
        spec = {'_id': timestamp}
        index = [('_id', pymongo.DESCENDING)]
    else:
        spec = {'ts': timestamp,
                'host': host}
        index = [('host', pymongo.ASCENDING),
                 ('ts', pymongo.DESCENDING)]

    if extra.get('custom_schema', False):
        doc = data
    else:
        doc = {'ts': timestamp,
               'host': host,
               'data': data}

    db = get_mongo_conn()
    coll = db['%s_%s' % (sourcetype, timestamp.strftime('%Y%m'))]
    coll.update(spec, doc, upsert=True, safe=True)
    coll.ensure_index(index, background=True)

    return "ok"


if __name__ == '__main__':
    app.run(host=app.config['HOST'], port=app.config['PORT'])

