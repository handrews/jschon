import urllib.parse

import pytest

from jschon import Catalog, CatalogError, create_catalog, JSON, JSONPointer, URI
from jschon.resource import (
    JSONResource,
    ResourceURIs,
    ResourceError,
    ResourceNotReadyError,
    BaseURIConflictError,
    ResourceURINotSetError,
    RelativeResourceURIError,
)

class TooSoon(JSONResource):
    def __init__(self, *args, **kwargs):
        # Generate ResourceNotReadyError
        self._pre_recursion_init(*args, **kwargs)


class UnSet(JSONResource):
    def _pre_recursion_init(self, *args, **kwargs):
        # Tests accessing properties before initialization
        pass


class PreCalculatePointerUri(JSONResource):
    def _pre_recursion_init(self, *args, **kwargs):
        # It is otherwise difficult to pass a non-root JSON Pointer fragment
        # URI without producing a document tree with inconsistent state.
        if not self.is_resource_root():
            kwargs['uri'] = ResourceURIs.pointer_uri_for(self)
        super()._pre_recursion_init(*args, **kwargs)


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
            # Reverse from the expected arrangement to test that
            # the outcome remains the same.
            tmp = uri
            uri = anchor_uris[0]
            anchor_uris[0] = tmp

        additional_uris = set(anchor_uris)
        self.base_uri_from_init = base_uri

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


# JSONResource to test the actual defaults of the base class,
# FakeSchema because its is_resource_root() accesses parent information.
@pytest.mark.parametrize('cls', (JSONResource, FakeSchema))
def test_constructor_defaults(cls):
    input_value = {'foo': ['bar']}
    jr = cls(input_value)

    assert type(jr) is cls
    assert jr.path == JSONPointer('')
    assert jr.parent is None
    assert jr.parent_in_resource is None
    assert jr.resource_root is jr
    assert jr.is_resource_root() is True

    assert type(jr['foo']) is cls
    assert jr['foo'].path == JSONPointer('/foo')
    assert jr['foo'].parent is jr
    assert jr['foo'].parent_in_resource is jr['foo'].parent
    assert jr['foo'].resource_root is jr
    assert jr['foo'].is_resource_root() is False

    assert type(jr['foo'][0]) is cls
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


def test_catalog_and_cacheid():
    foo_cat = create_catalog('2019-09', name='foo')

    r1 = JSONResource({}, catalog=foo_cat)
    assert r1.catalog == foo_cat

    r2 = JSONResource({}, catalog='foo', cacheid='bar')
    assert r2.catalog == foo_cat
    assert r2.cacheid == 'bar'


def test_resource_not_ready():
    with pytest.raises(ResourceNotReadyError):
        TooSoon()

@pytest.mark.parametrize(
    'attr',
    ('uri', 'pointer_uri', 'base_uri'),
)
def test_uri_not_set(attr):
    r = UnSet({})
    with pytest.raises(ResourceURINotSetError):
        getattr(r, attr)


def test_relative_base_error():
    with pytest.raises(RelativeResourceURIError):
        JSONResource({}, uri=URI('/foo/bar'))


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
            URI('https://ex.org'),
            {URI('https://ex.org#baz')},
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
    assert r.pointer_uri == base_uri.copy(fragment=pointer.uri_fragment())
    assert r.base_uri_from_init == base_uri
    assert r.base_uri == base_uri

    assert register_uri == (property_uri in catalog._schema_cache['default'])
    assert catalog.get_resource(property_uri, cls=FakeSchema) is r
    for au in additional_uris:
        assert catalog.get_resource(au) is r


def test_non_root_json_pointer_uri():
    # This cannot be covered using FakeSchema
    base = URI('tag:example.com,2023:base')
    r = PreCalculatePointerUri({'foo': 'bar'}, uri=base)
    assert r.uri == base
    assert r['foo'].uri == base.copy(fragment=r['foo'].path.uri_fragment())


@pytest.fixture
def mixed_document():
    root_node = JSONResource({})
    root_node['plain_node'] = JSON({}, itemclass=JSONResource)
    root_node['plain_node']['resource_node'] = JSONResource({})
    assert root_node.path == JSONPointer()
    assert root_node['plain_node'].path == JSONPointer('/plain_node')
    assert root_node['plain_node']['resource_node'].path == JSONPointer('/plain_node/resource_node')

    return root_node


def test_reassign_root_uri_with_children(catalog):
    original = URI('tag:example.com,2023:one')
    new = URI('tag:example.com,2023:two')

    # Need to test children of both objects and arrays,
    # and test at least 2 levels deep.
    r = JSONResource({'foo': ['bar']}, uri=original)
    r.uri = new

    assert r.uri == new
    assert r.pointer_uri == new.copy(fragment='')
    assert r.base_uri == new
    assert r.additional_uris == frozenset()
    assert catalog.get_resource(new) is r
    with pytest.raises(CatalogError):
        catalog.get_resource(original)

    assert r['foo'].uri == new.copy(fragment='/foo')
    assert r['foo'].pointer_uri == r['foo'].uri
    assert r['foo'].base_uri == r.base_uri
    assert r['foo'].additional_uris == frozenset()
    assert catalog.get_resource(r['foo'].uri) is r['foo']
    with pytest.raises(CatalogError):
        catalog.get_resource(original.copy(fragment='/foo'))

    assert r['foo'][0].uri == new.copy(fragment='/foo/0')
    assert r['foo'][0].pointer_uri == r['foo'][0].uri
    assert r['foo'][0].base_uri == r.uri
    assert r['foo'][0].additional_uris == frozenset()
    assert catalog.get_resource(r['foo'][0].uri) is r['foo'][0]
    with pytest.raises(CatalogError):
        catalog.get_resource(original.copy(fragment='/foo/0'))


def test_base_uri_conflict():
    r = JSONResource({'foo': 42})
    with pytest.raises(BaseURIConflictError):
        r['foo'].uri = URI('https:not-a-urn.com#/foo')


def test_change_additional_uris(catalog):
    base_uri_str = "https://example.com/base"
    anchor_str = "a"
    dynamic_anchor_str = "b"

    base_uri = URI(base_uri_str)
    anchor_uri = base_uri.copy(fragment=anchor_str)
    dynamic_anchor_uri = base_uri.copy(fragment=dynamic_anchor_str)
    other_uri = URI("tag:example.com,2023:something-different")

    r = FakeSchema({
        "$id": base_uri_str,
        "$anchor": anchor_str,
        "$dynamicAnchor": dynamic_anchor_str,
    })
    # Recall that you cannot change the primary URI through additional_uris
    r.additional_uris = (
        r.additional_uris - {dynamic_anchor_uri, base_uri}
    ) | {other_uri}

    assert r.uri == base_uri
    assert catalog.get_resource(r.uri) is r
    assert catalog.get_resource(anchor_uri) is r
    assert catalog.get_resource(other_uri) is r
    with pytest.raises(CatalogError):
        catalog.get_resource(dynamic_anchor_uri)
