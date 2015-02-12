import os
import re

BROKER_URL = os.getenv("CLOUDAMQP_URL", 'amqp://')
# BROKER_POOL_LIMIT = None

MONGOLAB_URI = None
MONGOLAB_DB = None
URI_WITH_AUTH = None

mongolab = os.getenv("MONGOLAB_URI")
if mongolab is not None:
    uri_pat = r"mongodb://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)"
    user, passwd, host, port, db = re.match(uri_pat, mongolab).groups()
    uri = "mongodb://{}:{}".format(host, port)
    MONGOLAB_URI = uri
    MONGOLAB_DB = db
    CELERY_RESULT_BACKEND = uri
    CELERY_MONGODB_BACKEND_SETTINGS = {
        'database': db,
        'user': user,
        'password': passwd
    }

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
