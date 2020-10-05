import glob
from celery import group
import tasks
def read_file(file):
    for line in file:
        # split all tweets to single tweet
        yield line

filepath = './data/*'
files = glob.glob(filepath)

for file in files[0:1]:
    data = read_file(open(file, "r"))
    tweets = group(tasks.get_valid_json.s(line) for line in data)()
    tweets.get()
