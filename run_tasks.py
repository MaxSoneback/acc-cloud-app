import glob
from celery import group, chain, chord
import tasks

def extract_and_map_tweets(filepath):
    f = open(filepath, "r")

    #Save list of all chains for a file
    chains = []
    for line in f:
        #Validate that the line is a json-object, only save non-retweets and map all pronouns
        tweet = chain(tasks.validate_line.s(line) | tasks.remove_retweet.s() | tasks.map.s() )
        chains.append(tweet)
    return group(*chains)

#Create list of all filepaths
filepath = './data/*'
files = glob.glob(filepath)

#Extract tweets from file and map them
mapped_tweets = extract_and_map_tweets(files[0])

#Reduce the mappings
results = chord(mapped_tweets)(tasks.reduce.s())
results.get()
