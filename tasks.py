from collections import Counter
from cel import app
from celery import group
import json
from json import JSONDecodeError

@app.task
def extract_tweets(filepath):
    f = open(filepath, "r")
    res = []
    for line in f:
        tweet = (validate_line.s(line) | remove_retweet.s())()
        res.append(tweet)
    return group(res)

@app.task(bind=True)
def validate_line(self, line):
    try:
        valid_json = json.loads(line)
        return valid_json
    except JSONDecodeError:
        self.request.callbacks = None

@app.task(bind=True)
def remove_retweet(self, tweet):
    try:
        tweet["retweeted_status"]
        self.request.callbacks = None
    except KeyError:
        return tweet

@app.task
def map(tweet):
    counter = Counter()
    for word in tweet:
        if word not in c:
            c[word] = 0
            c[word] += 1
    return c

@app.task
def reduce(counters):
    res = counters[0]
    for c in counters[1:]:
        res += c
    return res
