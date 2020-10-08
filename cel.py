import celery
from celery import Celery, group, states
from celery.backends.redis import RedisBackend

def patch_celery():
    """Patch redis backend."""
    def _unpack_chord_result(
            self, tup, decode,
            EXCEPTION_STATES=states.EXCEPTION_STATES,
            PROPAGATE_STATES=states.PROPAGATE_STATES,
                                                ):  
        _, tid, state, retval = decode(tup)
        
        if state in EXCEPTION_STATES:
            retval = self.exception_to_python(retval)
            if state in PROPAGATE_STATES:
                # retval is an Exception 
                return '{}: {}'.format(retval.__class__.__name__, str(retval))
            return retval
        celery.backends.redis.RedisBackend._unpack_chord_result = _unpack_chord_result
        return celery

app = patch_celery().Celery('proj',
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
