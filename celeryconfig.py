result_backend = 'rpc://'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Europe/Stockholm'
enable_utc = True
task_routes = {
            'tasks.add': 'low-priority',
            }
