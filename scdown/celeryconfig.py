import os

BROKER_URL = os.getenv("CLOUDAMQP_URL", 'amqp://')
BROKER_POOL_LIMIT = None

MONGOLAB_URI = None
MONGOLAB_DB = None

mongolab = os.getenv("MONGOLAB_URI")
if mongolab is not None:
    uri, _, db = mongolab.rpartition('/')
    MONGOLAB_URI = uri
    MONGOLAB_DB = db
    CELERY_RESULT_BACKEND = uri
    CELERY_MONGODB_BACKEND_SETTINGS = {
      'database': db,
    }

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
