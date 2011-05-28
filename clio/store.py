
from datetime import datetime

import pymongo
from flask import Flask, request
app = Flask(__name__)
app.config.from_object('clio.settings')
try:
    app.config.from_envvar('CLIO_SETTINGS')
except RuntimeError:
    pass


@app.route("/store/<host>/<sourcetype>/<float:timestamp>", methods=['PUT'])
def store(host, sourcetype, timestamp):
    db = pymongo.Connection(app.config['MONGO_HOSTS'], app.config['MONGO_PORT']).clio
    data = request.data
    timestamp = datetime.utcfromtimestamp(timestamp)
    app.logger.info("host: %s, sourcetype: %s, timestamp: %s" % (host, sourcetype, timestamp))
    coll = db['_%i' % timestamp.year]['_{0:0>2}'.format(timestamp.month)][sourcetype]
    coll.update({
        'ts': timestamp,
        'host': host},
        {
        'ts': timestamp,
        'host': host,
        'data': data},
        upsert=True,
        safe=True)

    return 'done'

if __name__ == '__main__':
    app.run(host=app.config['HOST'], port=app.config['PORT'])

