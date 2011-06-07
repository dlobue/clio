
import json
from bson import json_util

from flask.wrappers import Request, cached_property

def getBoolean(string):
    if string is None:
        return False
    return {
        '1': True, 'yes': True, 'true': True, 'on': True,
        '0': False, 'no': False, 'false': False, 'off': False, '': False, None: False
    }[string.lower()]


class ExtRequest(Request):
    @cached_property
    def json(self):
        """If the mimetype is `application/json` this will contain the
        parsed JSON data.
        """
        if self.mimetype in ('application/json','application/extjson'):
            if 'ext' in self.mimetype:
                objhook = json_util.object_hook
            else:
                objhook = None

            request_charset = self.mimetype_params.get('charset')
            if request_charset is not None:
                j = json.loads(self.data, encoding=request_charset, object_hook=objhook )
            else:
                j = json.loads(self.data, object_hook=objhook)

            return j


