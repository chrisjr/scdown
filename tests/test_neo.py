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
from py2neo import Graph
from itertools import product
from nose.tools import with_setup

TEST_DB = "http://127.0.0.1:8585/db/data/"
graph = Graph(TEST_DB)
neo = Neo(graph)


def setup_func():
    pass


def teardown_func():
    graph.delete_all()


datum = {"id": 1, "name": "Me"}
datum2 = dict(datum)
nested = {"new": {"data": True, "deeply": "nested"}}
datum2["novum"] = nested


def test_deflate():
    flat = neo.deflate(datum2)
    # adds keys due to nesting
    assert len(flat) == len(datum2) + 1
    # idempotent
    assert flat == neo.deflate(flat)


@with_setup(setup_func, teardown_func)
def test_create_or_update_node():
    datum = {"id": 1, "name": "Me"}
    datum1 = dict(datum)
    datum1["color"] = "red"
    node = neo.create_or_update_node(NODE_USER, datum)
    assert node.exists
    assert NODE_USER in node.labels
    node2 = neo.create_or_update_node(NODE_USER, datum1)
    assert node.ref == node2.ref
    assert node.properties == datum1


@with_setup(setup_func, teardown_func)
def test_node_types():
    nodes = set()
    for n in [NODE_USER, NODE_COMMENT, NODE_PROFILE, NODE_TRACK]:
        node = neo.create_or_update_node(n, datum)
        nodes.add(node)
    assert len(nodes) == 4


@with_setup(setup_func, teardown_func)
def test_relation_types():
    nodes = {}
    acceptable = set(
        [(NODE_USER, REL_HAS_PROFILE, NODE_PROFILE),
         (NODE_USER, REL_FOLLOWS, NODE_USER),
         (NODE_USER, REL_UPLOADED, NODE_TRACK),
         (NODE_USER, REL_FAVORITED, NODE_TRACK),
         (NODE_USER, REL_WROTE, NODE_COMMENT),
         (NODE_COMMENT, REL_REFERS_TO, NODE_TRACK)])
    accepted = set()
    rel_types = [REL_FOLLOWS,
                 REL_UPLOADED,
                 REL_FAVORITED,
                 REL_HAS_PROFILE,
                 REL_WROTE,
                 REL_REFERS_TO]
    for n in [NODE_USER, NODE_COMMENT, NODE_PROFILE, NODE_TRACK]:
        node = neo.create_or_update_node(n, datum)
        nodes[n] = node
    combos = product(nodes.items(), repeat=2)
    for c1, c2 in (tuple(prod) for prod in combos):
        k1, n1 = c1
        k2, n2 = c2
        for r in rel_types:
            try:
                neo.mk_relation(n1, r, n2)
                accepted.add((k1, r, k2))
            except AssertionError:
                pass
    assert acceptable == accepted


@with_setup(setup_func, teardown_func)
def test_nested_properties():
    node = neo.create_or_update_node(NODE_COMMENT, datum2)
    assert node.exists
    assert "novum" in node.properties
    assert node.properties["__json_novum"]
    assert neo.inflate(node.properties) == datum2
