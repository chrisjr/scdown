from scdown.s3 import dummyS3
from nose.tools import with_setup
from shutil import rmtree
import tempfile
import os

s3 = dummyS3()
store_dir = s3._s3_bucket.store_dir


def setup_func():
    if not os.path.exists(store_dir):
        os.makedirs(store_dir)


def teardown_func():
    if store_dir is not None:
        rmtree(store_dir)


@with_setup(setup_func, teardown_func)
def test_dummyS3():
    with tempfile.NamedTemporaryFile() as stream:
        stream.write("hello\n")
        stream.flush()
        stream.seek(0)
        s3.put_stream_in_s3("test", stream)
    fpath = os.path.join(store_dir, "test")
    assert os.path.exists(fpath)
    with open(fpath) as f:
        contents = f.read()
    assert contents == "hello\n"
