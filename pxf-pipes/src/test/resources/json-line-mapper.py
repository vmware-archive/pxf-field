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

    created_at = getValueOrEmpty(data, "created_at")
    id_str = getValueOrEmpty(data, "id_str")
    text = getValueOrEmpty(data, "text")
    source = getValueOrEmpty(data, "source")

    try:
        user_id = data["user"]["id"]
    except:
        screen_name = ""

    try:
        user_location = data["user"]["location"]
    except:
        hashtag = ""

    try:
        lat = data["coordinates"]["coordinates"][0]
    except:
        lat = ""

    try:
        lon = data["coordinates"]["coordinates"][1]
    except:
        lon = ""
        
    print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (created_at, id_str, text, source, user_id, user_location, lat, lon)
