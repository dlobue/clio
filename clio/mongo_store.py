
import pymongo

def mongodb_process_standard_records(coll, sourcetype, index_name, extra, host, records):
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
    coll.insert((_schema(ts,data) for ts,data in records))

def mongodb_process_timestamp_as_id(coll, sourcetype, index_name, records):
    index = [('_id', pymongo.DESCENDING)]

    for timestamp,data in records:
        spec = {'_id': timestamp}

        if hasattr(data, '__iter__') and not hasattr(data, 'setdefault'):
            data = { '$each': data }
        data = {'$addToSet':
                {'data': data }
               }


        coll.ensure_index(index, background=True)
        coll.update(spec, data, upsert=True)

