import urllib.parse

import pytest

from jschon import Catalog, JSONPointer, URI
from jschon.resource import (
    JSONResource,
    ResourceError,
    ResourceNotReadyError,
    ResourceURINotSetError,
    RelativeResourceURIError,
)

class FakeSchema(JSONResource):
    # For testing purposes, this simulates handling "$id", "$anchor",
    # and "$dynamicAnchor" through the constructor.  The JSONSchema
    # class handles these keywords in a very different way, which is
    # also expected to work and is tested by test_schema.py and the
    # JSON Schema Test Suite.
    def __init__(self, value, *args, parent=None, uri=None, **kwargs):
        if uri is not None:
            assert uri.scheme

            # We treat the provided URI as either the request URI (for
            # a root node and when no "$id" is present) or the URI of the
            # enclosing resource (if "$id" is present in an ancestor node).
            # These cases are the correct sources for an initial base URI
            # per RFC 3986 §5.1.3 and 5.1.2, respectively..
            base_uri = (
                parent.base_uri if parent is not None
                else uri.copy(fragment=None)
            )
        else:
            base_uri = None

        additional_uris = set()
        if "$id" in value:
            id_uri_ref = URI(value["$id"])
            assert id_uri_ref.fragment in (None, '')

            if base_uri is None:
                raise ValueError('FakeSchema: Cannot resolve relative $id')

            id_uri = id_uri_ref.resolve(base_uri)
            if id_uri != uri:
                additional_uris.add(uri)
                uri = id_uri
                base_uri = id_uri.copy(fragment=None)

        for frag_kwd in ("$anchor", "$dynamicAnchor"):
            if frag_kwd in value:
                additional_uris.add(
                    base_uri.copy(
                        fragment=urllib.parse.quote(
                            value[frag_kwd],
                            safe="!$&'()*+,;=@:/?",
                        )
                    )
                )

        if kwargs.get('itemclass') is None:
            kwargs['itemclass'] = FakeSchema
        super().__init__(
            value,
            *args,
            parent=parent,
            uri=uri,
            additional_uris=additional_uris,
            **kwargs,
        )

    def is_resource_root(self):
        return "$id" in self.data or self.parent_in_resource is None


def test_constructor_defaults():
    input_value = {'foo': ['bar']}
    jr = FakeSchema(input_value)

    assert type(jr) is FakeSchema
    assert jr.path == JSONPointer('')
    assert jr.parent is None
    assert jr.parent_in_resource is None
    assert jr.resource_root is jr
    assert jr.is_resource_root() is True

    assert type(jr['foo']) is FakeSchema
    assert jr['foo'].path == JSONPointer('/foo')
    assert jr['foo'].parent is jr
    assert jr['foo'].parent_in_resource is jr['foo'].parent
    assert jr['foo'].resource_root is jr
    assert jr['foo'].is_resource_root() is False

    assert type(jr['foo'][0]) is FakeSchema
    assert jr['foo'][0].path == JSONPointer('/foo/0')
    assert jr['foo'][0].parent is jr['foo']
    assert jr['foo'][0].parent_in_resource is jr['foo'][0].parent
    assert jr['foo'][0].resource_root is jr
    assert jr['foo'][0].is_resource_root() is False

    assert jr.catalog == Catalog.get_catalog()
    assert jr.cacheid == 'default'
    assert jr.references_resolved is True
    assert jr.value == input_value

    assert jr.uri.scheme == 'urn'
    assert jr.uri.path.startswith('uuid:')
    assert jr.uri.query is None
    assert jr.uri.fragment is None
    assert jr.pointer_uri.fragment == ''
    assert jr.pointer_uri.copy(fragment=None) == jr.uri
    assert jr.base_uri == jr.uri
    assert jr.additional_uris == frozenset()

    assert jr['foo'].base_uri == jr.base_uri
    assert jr['foo'].pointer_uri == jr.base_uri.copy(
        fragment=JSONPointer('/foo').uri_fragment(),
    )
    assert jr['foo'].uri == jr['foo'].pointer_uri
    assert jr['foo'].additional_uris == frozenset()

    assert jr['foo'][0].base_uri == jr.base_uri
    assert jr['foo'][0].pointer_uri == jr.base_uri.copy(
        fragment=JSONPointer('/foo/0').uri_fragment(),
    )
    assert jr['foo'][0].uri == jr['foo'][0].pointer_uri
    assert jr['foo'][0].additional_uris == frozenset()

# @pytest.mark.parametrize(
#     'document,pointer,input_uri,catalog_uris,property_uri,base_uri',
#     (
#         (
#             {},
#             JSONPointer(),
#             URI('https://ex.org'),
#             {URI('https://ex.org')},
#             URI('https://ex.org'),
#             URI('https://ex.org'),
#         ),
#         (
#             {},
#             JSONPointer(),
#             URI('https://ex.org#'),
#             {URI('https://ex.org')},
#             URI('https://ex.org'),
#             URI('https://ex.org'),
#         ),
#         (
#             {"things": {"foo": {}}},
#             JSONPointer('/things/foo'),
#             URI('https://ex.org#/things/foo'),
#             set(),
#             URI('https://ex.org#/things/foo'),
#             URI('https://ex.org'),
#         ),
#         (
#             {"things": {"foo": {}}},
#             JSONPointer('/things/foo'),
#             URI('https://ex.org#bar'),
#             {URI('https://ex.org#bar')},
#             URI('https://ex.org#bar'),
#             URI('https://ex.org'),
#         ),
#         (
#             {},
#             JSONPointer(),
#             URI('https://ex.org#bar'),
#             {URI('https://ex.org'), URI('https://ex.org#bar')},
#             URI('https://ex.org#bar'),
#             URI('https://ex.org'),
#         ),
#     ),
# )
# def test_uris(
#     document,
#     pointer,
#     input_uri,
#     catalog_uris,
#     property_uri,
#     base_uri,
#     catalog,
# ):
#     full = JSONResource(document, uri=input_uri)
#     r = pointer.evaluate(full)
#     assert r._catalog_uris == catalog_uris
#     assert r.uri == property_uri
#     assert r.pointer_uri == base_uri.copy(fragment=pointer.uri_fragment())
#     assert r.base_uri == base_uri
#     assert base_uri in document.cata
# 
#     for cu in catalog_uris:
#         assert catalog.get_resource(cu) is r
#     assert catalog.get_resource(r.uri) is r
