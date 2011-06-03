
def getBoolean(string):
    return {
        '1': True, 'yes': True, 'true': True, 'on': True,
        '0': False, 'no': False, 'false': False, 'off': False, '': False, None: False
    }[string.lower()]

