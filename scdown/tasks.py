from __future__ import absolute_import

import time
from celery import group, subtask, Task
from celery.utils.log import get_task_logger
from scdown.celery import app
from scdown.sc import (USER, USER_TRACKS,
                       USER_FOLLOWINGS, USER_FOLLOWERS,
                       USER_WEB_PROFILES,
                       TRACK_COMMENTS,
                       TRACK_FAVORITERS,
                       Sc)
from scdown.s3 import S3
from scdown.neo import (Neo,
                        NODE_USER,
                        NODE_TRACK,
                        NODE_COMMENT,
                        NODE_PROFILE,
                        REL_FOLLOWS,
                        REL_UPLOADED,
                        REL_FAVORITED,
                        REL_HAS_PROFILE,
                        REL_WROTE,
                        REL_REFERS_TO)


logger = get_task_logger(__name__)


class DatabaseTask(Task):
    abstract = True
    _neo = None
    _sc = None
    _s3 = None

    @property
    def neo(self):
        if self._neo is None:
            self._neo = Neo()
        return self._neo

    @property
    def sc(self):
        if self._sc is None:
            self._sc = Sc(logger=logger)
        return self._sc

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = S3(logger=logger)
        return self._s3


def now():
    return long(time.time())


def process_user(user_id):
    retrieve = fetch.s(USER, user_id) | store.s(res_type=NODE_USER)
    store_user_list = store_list.s(node_type=NODE_USER)
    get_followings = fetch_from.s(template=USER_FOLLOWINGS)
    get_followers = fetch_from.s(template=USER_FOLLOWERS)
    get_profiles = fetch_from.s(template=USER_WEB_PROFILES)
    get_tracks = fetch_from.s(template=USER_TRACKS)
    get_comments = fetch_from.s(template=TRACK_COMMENTS)
    get_favoriters = fetch_from.s(template=TRACK_FAVORITERS)

    followed = (get_followers | store_user_list |
                relate.s(rel_type=REL_FOLLOWS, timestamp=True))
    follows = (get_followings | store_user_list |
               relate.s(rel_type=REL_FOLLOWS, reverse=True, timestamp=True))
    web = (get_profiles | store_list.s(NODE_PROFILE) |
           relate.s(rel_type=REL_HAS_PROFILE, timestamp=True))
    tracks = (get_tracks | store_list.s(NODE_TRACK) |
              relate.s(rel_type=REL_UPLOADED))
    cmnts = foreach.s(callback=(get_comments |
                      store_list.s(node_type=NODE_COMMENT) |
                      relate_comments.s()))
    favoriters = foreach.s(callback=(get_favoriters |
                           store_list.s(node_type=NODE_USER) |
                           relate.s(rel_type=REL_FAVORITED,
                                    reverse=True, timestamp=True)))
    download = foreach.s(callback=get_audio.s())
    track_g = group(cmnts, favoriters, download)

    (retrieve | group(followed, follows, web, tracks | track_g)).apply_async()


@app.task
def foreach(it, callback):
    # Apply a callback for each item in an iterator
    tasks = []
    for arg in it:
        st = subtask(callback)
        tasks.append(st.clone(args=[arg, ]))
    group(tasks).apply_async()


@app.task(base=DatabaseTask)
def fetch(template, _id):
    sc = fetch.sc
    return sc.get_sc(template, _id)


@app.task(base=DatabaseTask)
def store(res, res_type):
    neo = store.neo
    node = neo.create_or_update_node(res_type, res)
    return node._id


@app.task(base=DatabaseTask)
def fetch_from(node_id, template=None):
    sc = fetch_from.sc
    neo = fetch_from.neo
    node = neo.get(node_id)
    remote_id = node.properties["id"]
    return (node_id, sc.get_sc(template, remote_id))


@app.task(base=DatabaseTask)
def store_list(node_and_reslst, node_type):
    neo = store_list.neo
    main_node_id, reslst = node_and_reslst
    node_ids = [neo.create_or_update_node(node_type, x)._id
                for x in reslst]
    return (main_node_id, node_ids)


@app.task(base=DatabaseTask)
def relate(x_ys, rel_type=None, reverse=False, timestamp=False):
    neo = relate.neo
    x_id, y_ids = x_ys
    x = neo.get(x_id)
    ys = [neo.get(y) for y in y_ids]
    props = {"as_of": now()} if timestamp else {}
    if reverse:
        rels = [neo.mk_relation(y, rel_type, x, props=props) for y in ys]
    else:
        rels = [neo.mk_relation(x, rel_type, y, props=props) for y in ys]
    neo.create_all(rels)
    return y_ids


@app.task(base=DatabaseTask)
def get_audio(track_node_id):
    neo = get_audio.neo
    track_node = neo.get(track_node_id)
    if "s3_key" in track_node.properties:
        return
    url = None
    if track_node.properties["downloadable"]:
        url = track_node.properties["download_url"]
    elif track_node.properties["streamable"]:
        url = track_node.properties["stream_url"]

    if url is not None:
        chain = (store_in_s3.s(track_node_id, url) |
                 save_s3_link.s())
        chain.apply_async()


@app.task(base=DatabaseTask)
def store_in_s3(track_node_id, url):
    sc = store_in_s3.sc
    s3 = store_in_s3.s3
    neo = store_in_s3.neo
    track_node = neo.get(track_node_id)
    track_id = track_node.properties["id"]
    fname = "{}.mp3".format(track_id)
    if s3.check_s3_for(fname):
        logger.info("Found {} on S3".format(fname))
        return (None, None)
    else:
        stream = sc.get_sc(url)
        return (track_node_id, s3.put_stream_in_s3(fname, stream))


@app.task(base=DatabaseTask, ignore_result=True)
def save_s3_link(track_s3):
    track_node_id, s3_key = track_s3
    if s3_key is not None:
        neo = save_s3_link.neo
        track_node = neo.get(track_node_id)
        track_id = track_node.properties["id"]
        props = {"id": track_id, "s3_key": s3_key}
        neo.create_or_update_node("Track", props)


@app.task(base=DatabaseTask, ignore_result=True)
def relate_comments(track_comments):
    neo = relate_comments.neo
    track_node_id, comment_node_ids = track_comments
    track_node = neo.get(track_node_id)
    comment_nodes = [neo.get(c) for c in comment_node_ids]
    user_comments = [
        (neo.create_or_update_node(NODE_USER,
                                   neo.inflate(c.properties)["user"]),
         c)
        for c in comment_nodes]
    refs = [neo.mk_relation(c, REL_REFERS_TO, track_node)
            for c in comment_nodes]
    writes = [neo.mk_relation(u, REL_WROTE, c)
              for u, c in user_comments]
    neo.create_all(refs + writes)
