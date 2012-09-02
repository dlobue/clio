
import calendar
from json import loads as json_loads
from cStringIO import StringIO
from cPickle import dumps as pickle_dumps
from hashlib import sha1
from struct import unpack_from, error

import logging

logger = logging.getLogger('clio')

from bson.json_util import object_hook


def _iter_records(dataframe, pos):
    while 1:

        try:
            recordlength = unpack_from('>L', dataframe, pos)[0]
        except error:
            break
        pos += 4
        recordbuf = buffer(dataframe, pos, recordlength)
        pos += recordlength

        timestamp,data = json_loads(str(recordbuf),
                                    object_hook=object_hook)
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

            recordid = [str(int(calendar.timegm( timestamp.timetuple() ))), host]
            if multi:
                if multi in data:
                    key_part =  data[multi]
                else:
                    key_part = sha1( pickle_dumps(data) ).hexdigest()
                recordid.append( key_part )
            recordid = ':'.join(recordid)

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

def process(dataframe):
    header_size = unpack_from('>L', dataframe, 0)[0]
    headerbuf = buffer(dataframe, 4, header_size)
    data = json_loads(str(headerbuf), object_hook=object_hook)
    host,sourcetype,extra = data
    logger.info("host: %s, sourcetype: %s" % (host, sourcetype))

    index_name = 'clio_%s' % extra['started_timestamp'].strftime('%Y%m')

    records = _iter_records(dataframe, header_size+4)
    if extra.get('timestamp_as_id', False):
        extra['multi'] = True
        records = reformat_records(records)
    return process_records(sourcetype, index_name, extra, host, records)



