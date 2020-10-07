import glob
from celery import group
import tasks

filepath = './data/*'
files = glob.glob(filepath)
data = tasks.extract_tweets.delay(files[0])
data.get()
