result_backend = 'redis://localhost:6379/0'
worker_prefetch_multiplier = 2
ignore_result = False
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Europe/Stockholm'
enable_utc = True
task_routes = {
            'proj.tasks.validate_json': {'queue': 'celery', 'delivery_mode': 'transient'},
            'proj.tasks.remove_retweet': {'queue': 'celery', 'delivery_mode': 'transient'},
            'proj.tasks.map': {'queue': 'celery', 'delivery_mode': 'transient'}
            }
