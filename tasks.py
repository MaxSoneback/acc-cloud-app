from collections import Counter
from cel import app
import json
from json import JSONDecodeError

@app.task
def get_valid_json(line):
    try:
        valid_json = json.loads(line)
        return valid_json
    except JSONDecodeError:
        pass

@app.task
def get_tweets(doc):
    for line in doc:
        yield line

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
