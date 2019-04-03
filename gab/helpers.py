import re
import json

punctuationRE = re.compile(r'[.?!]')

def messagePrep(s):
    s = json.loads(s)
    return [e.split() for e in punctuationRE.split(x.lower())]

def isBefore(s):
    if int(s.split('-')[0]) < 2018:
        return True
    elif int(s.split('-')[1]) < 11:
        return True
    return False
