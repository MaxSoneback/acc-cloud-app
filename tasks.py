from collections import Counter
from app import celery
from celery import group
import json
from json import JSONDecodeError
import re
pattern = "(?<!\w)((?:[hH][aAoOeE][nN])|(?:[dD][Ee][tTnN])|(?:[Dd][Ee][Nn]{2}[AaEe]))(?!\w)"

@celery.task
def reduce(counters):
    res = Counter() 
    for counter in counters:  
            res.update(counter)
    return json.dumps(res, indent=4)

@celery.task
def count_in_file(filepath):
    
    c = Counter()
    c["total_tweets"] = 0
    c["pronoun_tweets"] = 0
    for line in read_file(open(filepath, 'r')):
        
        try:
            tweet = json.loads(line)
        
        except JSONDecodeError:
            pass

            try:
                # Retweets can be distinguished by the existence of key "retweeted_status". We don't want retweets
                tweet["retweeted_status"]
            
            except KeyError:
                matches = re.findall(rf"{pattern}", tweet["text"])
                c["total_tweets"] += 1
                if matches:
                    # To count the total number of unique tweets, map the value 1 to the key "tweet" for each tweet
                    c["pronoun_tweets"] += 1
                    
                    for match in matches:
                        if match.lower() not in c:
                            c[match.lower()] = 0
                        c[match.lower()] += 1
    return c

def read_file(f):
    for line in f:
        yield line
