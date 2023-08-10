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
        if uri is None:
            base_uri = None if parent is None else parent.base_uri
        else:
            assert uri.scheme
            base_uri = uri.copy(fragment=None)

        additional_uris = set()
        if "$id" in value:
            id_uri_ref = URI(value["$id"])

            assert id_uri_ref.fragment in (None, '')
            if id_uri_ref.scheme is None:
                if base_uri is None:
                    raise ValueError('FakeSchema: Cannot resolve relative $id')

                id_uri = id_uri_ref.resolve(base_uri)
            else:
                id_uri = id_uri_ref

            if id_uri != uri:
                if uri is not None:
                    additional_uris.add(uri)
            uri = id_uri
            base_uri = uri.copy(fragment=None)

        anchor_uris = []
        for frag_kwd in ("$anchor", "$dynamicAnchor"):
            if frag_kwd in value:
                anchor_uris.append(
                    base_uri.copy(
                        fragment=urllib.parse.quote(
                            value[frag_kwd],
                            safe="!$&'()*+,;=@:/?",
                        )
                    )
                )
        if uri is None and anchor_uris:
            # We don't have an absolute-URI, take the first anchor URI instead.
            uri = anchor_uris[0]
            anchor_uris = anchor_uris[1:]

        elif uri is not None and len(anchor_uris) == 1:
            # Reverse from the recommendation to test robustness
            tmp = uri
            uri = anchor_uris[0]
            anchor_uris[0] = tmp

        additional_uris = set(anchor_uris)

        assert None not in additional_uris

        assert (base_uri is None) or (base_uri.fragment is None)

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

@pytest.mark.parametrize(
    'document,pointer,register_uri,property_uri,pointer_uri,base_uri,additional_uris',
    (
        (
            {"$id": "https://ex.org"},
            JSONPointer(),
            True,
            URI('https://ex.org'),
            URI('https://ex.org#'),
            URI('https://ex.org'),
            frozenset(),
        ),
        (
            {"$id": "https://ex.org#"},
            JSONPointer(),
            True,
            URI('https://ex.org'),
            URI('https://ex.org#'),
            URI('https://ex.org'),
            frozenset(),
        ),
        (
            {"$id": "https://ex.org", "things": {"foo": {}}},
            JSONPointer('/things/foo'),
            False,
            URI('https://ex.org#/things/foo'),
            URI('https://ex.org#/things/foo'),
            URI('https://ex.org'),
            frozenset(),
        ),
        (
            {"$id": "https://ex.org#", "things": {"foo": {"$anchor": "bar"}}},
            JSONPointer('/things/foo'),
            True,
            URI('https://ex.org#bar'),
            URI('https://ex.org#/things/foo'),
            URI('https://ex.org'),
            frozenset(),
        ),
        (
            {"$id": "https://ex.org", "$anchor": "bar", "$dynamicAnchor": "baz"},
            JSONPointer(),
            True,
            URI('https://ex.org'),
            URI('https://ex.org#'),
            URI('https://ex.org'),
            {URI('https://ex.org#bar'), URI('https://ex.org#baz')},
        ),
        (
            {"$id": "https://ex.org#", "$dynamicAnchor": "baz"},
            JSONPointer(),
            True,
            URI('https://ex.org'),
            URI('https://ex.org#'),
            URI('https://ex.org#baz'),
            {URI('https://ex.org')},
        ),
    ),
)
def test_uris(
    document,
    pointer,
    register_uri,
    property_uri,
    pointer_uri,
    base_uri,
    additional_uris,
    catalog,
):
    full = FakeSchema(document)
    r = pointer.evaluate(full)

    assert r.uri == property_uri
    assert r.base_uri == base_uri
    assert r.pointer_uri == base_uri.copy(fragment=pointer.uri_fragment())

    assert register_uri == (property_uri in catalog._schema_cache['default'])
    assert catalog.get_resource(property_uri, cls=FakeSchema) is r
    for au in additional_uris:
        assert catalog.get_resource(au) is r
