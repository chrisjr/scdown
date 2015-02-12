from __future__ import absolute_import

from celery import Celery

app = Celery('scdown', include=['scdown.tasks'])
app.config_from_object('scdown.celeryconfig')

if __name__ == '__main__':
    app.start()
