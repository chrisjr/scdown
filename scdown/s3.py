import boto
from boto.s3.key import Key

import logging
import shutil
import os
from tempfile import mkdtemp, NamedTemporaryFile
from contextlib import closing

BUCKET_NAME = "chrisjr-scdown"


class DummyBucket(object):
    store_dir = None
    logger = None

    def __init__(self, bucket_name,
                 store_dir=None,
                 logger=logging.getLogger(__name__)):
        if store_dir is None:
            store_dir = mkdtemp(bucket_name)
        elif not os.path.exists(store_dir):
            os.makedirs(store_dir)
        self.store_dir = store_dir
        self.logger = logger
        self.logger.info("Storing to {}".format(self.store_dir))

    def get_key(self, keyname):
        pass

    def store_for_key(self, keyname, stream):
        fname = os.path.join(self.store_dir, keyname)
        self.logger.info("Writing {} to {}".format(keyname, fname))
        with open(fname, "wb") as f:
            shutil.copyfileobj(stream, f)


class DummyKey(object):
    bucket = None
    key = None
    logger = None

    def __init__(self, bucket, logger=logging.getLogger(__name__)):
        self.bucket = bucket
        self.logger = logger

    def set_contents_from_file(self, stream, **kwargs):
        self.logger.info(
            "Storing {} with args {}".format(self.key, kwargs))
        self.bucket.store_for_key(self.key, stream)

    def set_contents_from_filename(self, fname, **kwargs):
        with open(fname, 'rb') as f:
            self.set_contents_from_file(f, **kwargs)


class S3(object):
    _s3_bucket = None   # swap out implementation of bucket if needed
    _Key = None         # swap out implementation of key if needed
    _logger = None

    def __init__(self, _s3_bucket=None, _Key=None,
                 logger=logging.getLogger(__name__)):
        if _Key is None:
            _Key = Key
        self._Key = _Key
        if _s3_bucket is None:
            s3_conn = boto.connect_s3()
            self._s3_bucket = s3_conn.get_bucket(BUCKET_NAME, validate=False)
        else:
            self._s3_bucket = _s3_bucket
        self._logger = logger

    def check_s3_for(self, keyname):
        remote_k = self._s3_bucket.get_key(keyname)
        return remote_k is not None and remote_k.size > 0

    def put_stream_in_s3(self, keyname, stream):
        if stream is not None:
            with closing(stream) as s:
                k = self._Key(self._s3_bucket)
                k.key = keyname
                if hasattr(s, "raw"):
                    with NamedTemporaryFile(delete=False) as f:
                        temp_fname = f.name
                        shutil.copyfileobj(s.raw, f)
                    k.set_contents_from_filename(temp_fname,
                                                 reduced_redundancy=True)
                    os.unlink(temp_fname)
                else:
                    k.set_contents_from_file(s, reduced_redundancy=True)
                return keyname


def dummyS3(store_dir=None):
    bucket = DummyBucket(BUCKET_NAME, store_dir=store_dir)
    return S3(bucket, DummyKey)
