

import calendar
from json import loads as json_loads
from cStringIO import StringIO
from cPickle import dumps as pickle_dumps
from hashlib import sha1

import logging

logger = logging.getLogger('clio')






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


