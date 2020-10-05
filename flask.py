
from celery import Celery
from flask import Flask

def make_celery(app):
    celery = Celery(
            app.import_name,
            backend=app.config['CELERY_RESULT_BACKEND'],
            broker=app.config['CELERY_BROKER_URL'],
            include=['proj.tasks']
            )
    
    celery.conf.update(app.config)
            
    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
    
    celery.Task = ContextTask
    return celery

flask_app = Flask(__name__)
flask_app.config.update(
        CELERY_BROKER_URL='amqp://',
        CELERY_RESULT_BACKEND='rpc://'
                )
celery = make_celery(flask_app)

if __name__ == '__main__':
    flask_app.run()
