
from datetime import datetime

import pymongo
from flask import Flask, request
app = Flask(__name__)
app.config.from_object('clio.settings')
try:
    app.config.from_envvar('CLIO_SETTINGS')
except RuntimeError:
    app.logger.debug("couldn't load settings from file in envvar. trying /etc/clio.cfg")
    try:
        app.config.from_pyfile('/etc/clio.cfg')
    except RuntimeError:
        app.logger.debug("unable to find any settings files. using defaults in local settings module.")

from utils import getBoolean, ExtRequest
app.request_class = ExtRequest

@app.route("/store/<host>/<sourcetype>/<float:timestamp>", methods=['PUT'])
def store(host, sourcetype, timestamp):

    if request.headers.get('interval', None):
        interval = float(request.headers['interval'])
        offset = timestamp % interval
        timestamp = timestamp - offset

    timestamp = datetime.utcfromtimestamp(timestamp)

    if request.json is not None:
        data = request.json
    else:
        app.logger.debug("data was in raw format")
        data = request.data

    app.logger.info("host: %s, sourcetype: %s, timestamp: %s" % (host, sourcetype, timestamp))

    if request.headers.get('_id', None):
        spec = {'_id': request.headers['_id']}
    else:
        spec = {'ts': timestamp,
                'host': host}

    if getBoolean(request.headers.get('custom', None)):
        doc = data
    else:
        doc = {'ts': timestamp,
               'host': host,
               'data': data}

    db = pymongo.Connection(app.config['MONGO_HOSTS'], app.config['MONGO_PORT']).clio
    coll = db['_%i' % timestamp.year]['_{0:0>2}'.format(timestamp.month)][sourcetype]
    coll.update(spec, doc, upsert=True, safe=True)

    return ''


if __name__ == '__main__':
    app.run(host=app.config['HOST'], port=app.config['PORT'])

