# Stolen from http://paste.pocoo.org/show/224441/

from pymongo.cursor import Cursor
from pymongo.connection import Connection
from pymongo.errors import AutoReconnect

from time import sleep

def reconnect(f):
    def f_retry(*args, **kwargs):
        while True:
            try:
                return f(*args, **kwargs)
            except AutoReconnect, e:
                print('Fail to execute %s [%s]' % (f.__name__, e))
            sleep(0.1)
    return f_retry

Cursor._Cursor__send_message = reconnect(Cursor._Cursor__send_message)
Connection._send_message = reconnect(Connection._send_message)
Connection._send_message_with_response = reconnect(Connection._send_message_with_response)
Connection._Connection__find_master = reconnect(Connection._Connection__find_master)

