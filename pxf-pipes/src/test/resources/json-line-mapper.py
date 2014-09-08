#!/usr/bin/python

import json
import sys

def getValueOrEmpty(data, key):    
    try:
        return data[key]
    except (KeyError):
        return ""

for line in sys.stdin:
    idx = line.index('\t')
    key = line[0:idx].strip()
    value = line[idx+1:]

    data = json.loads(value)

    #print value

    created_at = getValueOrEmpty(data, "created_at")
    id = getValueOrEmpty(data, "id")
    text = getValueOrEmpty(data, "text")

    try:
        screen_name = data["user"]["screen_name"]
    except (KeyError):
        screen_name = ""

    try:
        hashtag = data["entities"]["hashtags"][0]
    except (KeyError, IndexError):
        hashtag = ""

    try:
        lat = data["coordinates"]["coordinates"][0]
    except (KeyError, IndexError):
        lat = ""

    try:
        lon = data["coordinates"]["coordinates"][1]
    except (KeyError, IndexError):
        lon = ""
        
    sys.stdout.write("%s|%s|%s|%s|%s|%s|%s" % (created_at, id, text, screen_name, hashtag, lat, lon))
