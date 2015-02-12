from py2neo import ServiceRoot, Relationship
import logging
import os
try:
    import json
except ImportError:
    import simplejson as json


ID_PROP = "id"

NODE_USER = "User"
NODE_TRACK = "Track"
NODE_COMMENT = "Comment"
NODE_PROFILE = "Profile"


UNIQUES = [(x, ID_PROP) for x in
           [NODE_USER, NODE_TRACK, NODE_COMMENT, NODE_PROFILE]]

# relationships:
# (a:User)-[:FOLLOWS]->(b:User) # timestamp
# (a:User)-[:UPLOADED]->(b:Track)
# (a:User)-[:FAVORITED]->(b:Track) # timestamp
# (a:User)-[:HAS_PROFILE]->(b:Profile) # timestamp
# (a:User)-[:WROTE]->(b:Comment)
# (a:Comment)-[:REFERS_TO]->(b:Track)


REL_FOLLOWS = "FOLLOWS"
REL_UPLOADED = "UPLOADED"
REL_FAVORITED = "FAVORITED"
REL_HAS_PROFILE = "HAS_PROFILE"
REL_WROTE = "WROTE"
REL_REFERS_TO = "REFERS_TO"


class Neo(object):
    _graph = None
    _extra_label = None
    logger = None

    def __init__(self, graph=None, logger=logging.getLogger("")):
        if graph is None:
            graphenedb_url = os.environ.get("GRAPHENEDB_URL",
                                            "http://localhost:7474/")
            graph = ServiceRoot(graphenedb_url).graph
        self._graph = graph
        self.logger = logger
        for (l, p) in UNIQUES:
            self.mk_unique(l, p)

    def mk_unique(self, label, property_key):
        schema = self._graph.schema
        if len(schema.get_uniqueness_constraints(label)) == 0:
            schema.create_uniqueness_constraint(label, property_key)

    def deflate(self, d):
        """Turn arbitrary dict into something Neo4j serializable"""
        new_d = {}
        for k, v in d.iteritems():
            if hasattr(v, "__iter__"):
                new_d["__json_" + k] = True
                new_d[k] = json.dumps(v)
            else:
                new_d[k] = v
        assert not any((isinstance(x, dict) or
                        isinstance(x, list) for x in new_d))
        return new_d

    def inflate(self, properties):
        """Turn Neo4j serialized dict into normal"""
        new_d = {}
        json_keys = [x for x in properties.keys()
                     if x.startswith("__json_")]
        for k in json_keys:
            orig_key = k[7:]
            new_d[orig_key] = json.loads(properties[orig_key])
        for k, v in properties.iteritems():
            if not k.startswith("__json_"):
                if k not in new_d:
                    new_d[k] = v
        return new_d

    def get(self, node_id):
        return self._graph.node(node_id)

    def create_or_update_node(self, item_type, item_properties):
        props = self.deflate(item_properties)
        item_id = props[ID_PROP]
        node = self._graph.merge_one(item_type, ID_PROP, item_id)
        if self._extra_label is not None:
            if self._extra_label not in node.labels:
                node.labels.add(self._extra_label)
                node.labels.push()
        if dict(node.properties) != props:
            node.properties.update(props)
            node.push()
        return node

    def check_relation(self, node1, relationship, node2):
        """Check that the relation is being made between
           objects of the right type."""

        if relationship == REL_FOLLOWS:
            assert (NODE_USER in node1.labels and
                    NODE_USER in node2.labels)
        elif relationship == REL_UPLOADED:
            assert (NODE_USER in node1.labels and
                    NODE_TRACK in node2.labels)
        elif relationship == REL_FAVORITED:
            assert (NODE_USER in node1.labels and
                    NODE_TRACK in node2.labels)
        elif relationship == REL_HAS_PROFILE:
            assert (NODE_USER in node1.labels and
                    NODE_PROFILE in node2.labels)
        elif relationship == REL_WROTE:
            assert (NODE_USER in node1.labels and
                    NODE_COMMENT in node2.labels)
        elif relationship == REL_REFERS_TO:
            assert (NODE_COMMENT in node1.labels and
                    NODE_TRACK in node2.labels)

    def mk_relation(self, node1, relationship, node2, props={}):
        self.check_relation(node1, relationship, node2)
        return Relationship(node1, relationship, node2, **props)

    def create_all(self, relations):
        self._graph.create_unique(*relations)
