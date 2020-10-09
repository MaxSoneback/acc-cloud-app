
from celery import Celery
from flask import Flask
import json
from json import JSONDecodeError
from celery import group
import re

def make_celery(app):
    celery = Celery(
            app.import_name,
            backend=app.config['CELERY_RESULT_BACKEND'],
            broker=app.config['CELERY_BROKER_URL'],
            include=['tasks']
            )
    celery.config_from_object('celeryconfig')
    celery.conf.update(app.config)
    app.conf.update(result_expires=3600)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
    
    celery.Task = ContextTask
    return celery

flask_app = Flask(__name__)
flask_app.config.update(
        CELERY_BROKER_URL='amqp://',
        CELERY_RESULT_BACKEND='redis://localhost:6379/0'
                )
celery = make_celery(flask_app)

@celery.task
def reduce(counters):
    res = Counter()
    for counter in counters:
        res.update(counter)
    return json.dumps(res, indent=4)

@celery.task
def count_in_file(filepath):
    pattern = "(?<!\w)((?:[hH][aAoOeE][nN])|(?:[dD][Ee][tTnN])|(?:[Dd][Ee][Nn]{2}[AaEe]))(?!\w)"
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

if __name__ == '__main__':
    flask_app.run()
