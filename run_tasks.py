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
    print('returning list of chains')
    return chains

#Create list of all filepaths
filepath = './data/*'
files = glob.glob(filepath)
print('extracting and mapping tweets')
#Extract tweets from file and map them
header = extract_and_map_tweets(files[0])
print('header created')
#Reduce the mappings
callback = tasks.reduce.s()
results = chord(header)(callback)
print('end')
print(results.get())
