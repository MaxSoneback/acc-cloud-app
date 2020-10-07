from collections import Counter
from cel import app
from celery import group, chain
import json
from json import JSONDecodeError
import re
pattern = "(?<!\w)((?:[hH][aAoOeE][nN])|(?:[dD][Ee][tTnN])|(?:[Dd][Ee][Nn]{2}[AaEe]))(?!\w)"

@app.task
def extract_tweets(filepath):
    f = open(filepath, "r")
    chains = []
    for line in f:
        tweet =chain(validate_line.s(line) | remove_retweet.s())
        chains.append(tweet)
    return group(*chains)

@app.task(bind=True)
def validate_line(self, line):
    try:
        valid_json = json.loads(line)
        return valid_json
    except JSONDecodeError:
        # Only keep valid tweets, i.e. not new-lines or broken json objects
        self.request.chain = None

@app.task(bind=True)
def remove_retweet(self, tweet):
    try:
        #If key 'retweeted_status' exists, discard the tweet
        tweet["retweeted_status"]
        self.request.chain = None
    except KeyError:
        return tweet

@app.task(bind=True)
def map(self, tweet):
    c = Counter()
    matches = re.findall(rf"{pattern}", tweet["text"])
    if matches:
        #To count the total number of unique tweets, map the value 1 to the key "tweet" for each tweet
        c["tweet"] = 1
        for match in matches:
            if match.lower() not in c:
                c[match.lower()] = 0
            c[match.lower()] += 1
    else:
        self.request.chain = None
    return c

@app.task
def reduce(counters):
    res = counters[0]
    for c in counters[1:]:
        res += c
    return res
