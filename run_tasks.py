import glob
from celery import group
import tasks

def extract_tweets(filepath):
    f = open(filepath, "r")
    chains = []
    for line in f:
        tweet = chain(tasks.validate_line.s(line) | tasks.remove_retweet.s())
        chains.append(tweet)
    return group(*chains)

filepath = './data/*'
files = glob.glob(filepath)
clean_tweets = extract_tweets(files[0])
results = clean_tweets.delay()
results.get()
