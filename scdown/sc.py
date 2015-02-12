import soundcloud
import os
import logging
from datetime import datetime
import requests
import sys
import tempfile
try:
    import json
except ImportError:
    import simplejson as json

from celeryconfig import CELERY_RESULT_BACKEND
import pymongo
from pymongo import MongoClient

USER = '/users/{_id}'
USER_TRACKS = '/users/{_id}/tracks'
USER_FOLLOWINGS = '/users/{_id}/followings'
USER_FOLLOWERS = '/users/{_id}/followers'
USER_WEB_PROFILES = '/users/{_id}/web-profiles'
TRACK = '/tracks/{_id}'
TRACK_COMMENTS = '/tracks/{_id}/comments'
TRACK_FAVORITERS = '/tracks/{_id}/favoriters'
TRACK_DOWNLOAD = '/tracks/{_id}/download'
TRACK_STREAM = '/tracks/{_id}/stream'


class RequestDB(object):
    client = None
    db = None
    coll = None
    logger = None

    def __init__(self, db_name="soundcloud", logger=logging.getLogger("")):
        self.logger = logger
        self.client = MongoClient(CELERY_RESULT_BACKEND)
        self.db = self.client[db_name]
        self.coll = self.db.requests
        self.coll.ensure_index([("key", pymongo.ASCENDING),
                               ("unique", True),
                               ("dropDups", True)])

    def get(self, key):
        v = self.coll.find_one({"key": key})
        if v is not None:
            return v["value"]
        else:
            return None

    def set(self, key, value):
        now = datetime.utcnow()
        doc = {"key": key, "value": value, "retrieved": now}
        self.coll.update({"key": key}, doc, upsert=True)
        self.logger.info("Stored {} in db".format(key))

    def close(self):
        if self.db is not None:
            self.db.close()


class Sc(object):
    _sc_client = None
    _db = None
    _logger = None

    def __init__(self, sc_client=None, db_name="soundcloud",
                 logger=logging.getLogger("")):
        self._logger = logger
        if sc_client is None:
            sc_client_id = os.getenv('SOUNDCLOUD_CLIENT_ID')
            if sc_client_id is None:
                err = "SOUNDCLOUD_CLIENT_ID was not set!"
                self._logger.error(err)
                sys.exit(err)
            sc_client = soundcloud.Client(client_id=sc_client_id)
        self._sc_client = sc_client

        self._db = RequestDB(db_name, logger)

    def get_sc(self, template, _id=None):
        key = template.format(_id=_id) if _id is not None else template
        self._logger.info("GET {}".format(key))
        value = self._db.get(key)
        if value is not None:
            return value
        else:
            if _id is None:
                res = self._sc_client.get(key, allow_redirects=False)
                track_url = res.location
                return requests.get(track_url, stream=True)
            else:
                res = self._sc_client.get(key)
                if hasattr(res, "data"):
                    res1 = [dict(o.fields()) for o in res]
                    self._db.set(key, res1)
                    return res1
                elif hasattr(res, "fields"):
                    res1 = dict(res.fields())
                    self._logger.info(repr(res1))
                    self._db.set(key, res1)
                    return res1
                else:
                    return res

    def __del__(self):
        if self._db is not None:
            self._db.close()


def prefill_user(user_id):
    """Cache the basic info on a user"""
    sc = Sc(db_name="soundcloud")
    for t in [USER, USER_WEB_PROFILES,
              USER_FOLLOWINGS, USER_TRACKS, USER_FOLLOWERS]:
        sc.get_sc(t, user_id)
