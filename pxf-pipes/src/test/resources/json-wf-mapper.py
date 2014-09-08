#!/usr/bin/python

import json
import sys

def getValueOrEmpty(data, key):
    
    try:
        return data[key]
    except (KeyError):
        return ""

bytes = sys.stdin.read()

idx = bytes.index('\t')
key = bytes[0:idx].strip()
value = bytes[idx+1:]
#print value
data = json.loads(value)

data["root"]
for record in data["root"]:
    recordData = record["record"]

    created_at = getValueOrEmpty(recordData, "created_at")
    id = getValueOrEmpty(recordData, "id")
    text = getValueOrEmpty(recordData, "text")

    try:
        screen_name = recordData["user"]["screen_name"]
    except (KeyError):
        screen_name = ""

    try:
        hashtag = recordData["entities"]["hashtags"][0]
    except (KeyError, IndexError):
        hashtag = ""

    try:
        lat = recordData["coordinates"]["coordinates"][0]
    except (KeyError, IndexError):
        lat = ""

    try:
        lon = recordData["coordinates"]["coordinates"][1]
    except (KeyError, IndexError):
        lon = ""
        
    sys.stdout.write("%s|%s|%s|%s|%s|%s|%s|%s\n" % (key, created_at, id, text, screen_name, hashtag, lat, lon))


