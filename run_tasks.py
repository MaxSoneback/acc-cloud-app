import glob
from celery import group, chain, chord
import tasks

def extract_and_map_tweets(filepaths, num_of_files):
    #save all chain signatures
    chains=[]
    for filepath in filepaths[0:num_of_files]:
        f = open(filepath, "r")

        for line in f:
            #Validate that the line is a json-object, only save non-retweets and map all pronouns
            tweet = chain(tasks.validate_line.s(line) | tasks.remove_retweet.s() | tasks.map.s() )
            chains.append(tweet)

    return group(*chains)

#Create list of all filepaths
filepath = './data/*'
files = glob.glob(filepath)
#Extract tweets from file and map them
header = extract_and_map_tweets(files, 2)
print('header created')
#Reduce the mappings
callback = tasks.reduce.s()
results = (header | callback)()
print(results.get())
