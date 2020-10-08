from celery import Celery

app = Celery('proj',
        broker='amqp://',
        backend='redis://localhost:6379/0',
        include=['tasks'])

# Optional configuration, see the application user guide.
app.config_from_object('celeryconfig')

app.conf.update(
            result_expires=3600,
            )

if __name__ == '__main__':
    app.start()
