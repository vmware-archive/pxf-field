#!/usr/bin/python

import sys

def getValueOrEmpty(data, key):
    try:
        return data[key]
    except (KeyError):
        return ""

bytes = sys.stdin.read()

idxKey = bytes.index('\t')
key = bytes[0:idxKey].strip()
bits = bytes[idxKey+1:]
print "%s|%s" % ("a", bits)

