import glob
from celery import group, chain, chord
import tasks

def extract_and_map_tweets(filepath):
    f = open(filepath, "r")
    chains = []
    for line in f:
        tweet = chain(tasks.validate_line.s(line) | tasks.remove_retweet.s() | tasks.map.s() )
        chains.append(tweet)
    return group(*chains)

filepath = './data/*'
files = glob.glob(filepath)
mapped_tweets = extract_and_map_tweets(files[0])
results = chord(mapped_tweets)(tasks.reduce.s())
results.get()
