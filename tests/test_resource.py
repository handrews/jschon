from functools import cached_property
from uuid import uuid4

import pytest

from jschon import Catalog, CatalogError, create_catalog, JSON, JSONPointer, URI
from jschon.resource import (
    JSONResource,
    JSONSchemaRefId,
    RefIdKeywordConfig,
    ResourceURIs,
    ResourceError,
    ResourceNotReadyError,
    ResourceURINotSetError,
    RelativeResourceURIError,
    DuplicateResourceURIError,
    UnRootedResourceError,
    InconsistentResourceRootError,
)


class UnSet(JSONResource):
    _tentative_uri = None
    _tentative_additional_uris = frozenset()
    _uri = None
    _base_uri = None
    _additional_uris = frozenset()
    def pre_recursion_init(self, *args, **kwargs):
        # Tests accessing properties before initialization
        pass


class PreCalculatePointerUri(JSONResource):
    def pre_recursion_init(self, *args, **kwargs):
        # It is otherwise difficult to pass a non-root JSON Pointer fragment
        # URI without producing a document tree with inconsistent state.
        if not self.is_resource_root():
            kwargs['uri'] = ResourceURIs.pointer_uri_for(self)
        super().pre_recursion_init(*args, **kwargs)


class AlternatingResource(JSONSchemaRefId):
    def __init__(self, *args, **kwargs):
        kwargs['itemclass'] = NonResourceSpacer
        super().__init__(*args, **kwargs)


class NonResourceSpacer(JSON):
    def __init__(self, *args, **kwargs):
        kwargs['itemclass'] = AlternatingResource
        super().__init__(*args, **kwargs)


# JSONResource to test the actual defaults of the base class,
# JSONSchemaRefId because its is_resource_root() accesses parent information.
@pytest.mark.parametrize('cls', (JSONResource, JSONSchemaRefId))
def test_constructor_defaults(cls):
    input_value = {'foo': ['bar']}
    jr = cls(input_value)

    assert type(jr) is cls
    assert jr.path == JSONPointer('')
    assert jr.parent is None
    assert jr.parent_in_resource is None
    assert jr.resource_root is jr
    assert jr.is_resource_root() is True
    assert list(jr.child_resource_nodes) == [jr['foo']]

    assert type(jr['foo']) is cls
    assert jr['foo'].path == JSONPointer('/foo')
    assert jr['foo'].parent is jr
    assert jr['foo'].parent_in_resource is jr['foo'].parent
    assert jr['foo'].resource_root is jr
    assert jr['foo'].is_resource_root() is False
    assert list(jr['foo'].child_resource_nodes) == [jr['foo'][0]]

    assert type(jr['foo'][0]) is cls
    assert jr['foo'][0].path == JSONPointer('/foo/0')
    assert jr['foo'][0].parent is jr['foo']
    assert jr['foo'][0].parent_in_resource is jr['foo'][0].parent
    assert jr['foo'][0].resource_root is jr
    assert jr['foo'][0].is_resource_root() is False
    assert list(jr['foo'][0].child_resource_nodes) == []

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


def test_override_pre_recursion_args():
    bar_cat = create_catalog('2020-12', name='bar')
    r = JSONResource({}, pre_recursion_args={'catalog': 'bar'})
    assert r.catalog == bar_cat


def test_resource_not_ready():
    class TooSoon(JSONResource):
        def __init__(self, *args, **kwargs):
            # Generate ResourceNotReadyError
            self.pre_recursion_init(*args, **kwargs)

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


def test_invalidate_unset_caches():
    r = UnSet({})

    # These should not throw
    # For _invalidate_path with pointer_uri, this only tests
    # the ResourceNotSetError path.  See test_invalidate_caches()
    # for testing the AttributeError path.
    assert r._invalidate_path() is None
    assert r._invalidate_value() is None


def test_invalidate_caches():
    # This produces:
    #   * a root node with a non-JSON Pointer uri attribute
    #   * a non-root node with a JSON Pointer uri attribute
    #   * a non-document-root resource root with a non-JSON Pointer uri attr
    r = JSONSchemaRefId([42, {"$id": "https://whatever.com"}])

    # Use id() because JSON.__eq__() tests for JSON value equality
    # and we want to verify node identity.
    get_attrs = lambda node: {
        'path': {
            'pointer_uri': node.pointer_uri,
            'resource_root': id(node.resource_root),
            'parent_in_resource': id(node.parent_in_resource),
        },
        'value': {
            'child_resource_nodes': [id(c) for c in node.child_resource_nodes],
            'child_resource_roots': [id(c) for c in node.child_resource_roots],
            'children_in_resource': [id(c) for c in node.children_in_resource],
        },
    }
    old_attrs = {}
    nodes = (r, r[0], r[1], r[1]['$id'])
    nodes = [r[0]]
    for node in nodes:
        # Nodes are not hashable, use id()
        old_attrs[id(node)] = get_attrs(node)

    path_attr_names = list(old_attrs.values())[0]['path'].keys()
    value_attr_names = list(old_attrs.values())[0]['value'].keys()

    for node in nodes:
        frag = node.uri.fragment
        frag_is_pointer = frag == '' or frag is not None and frag[0] == '/'

        for group, test_names, other_names in (
            ('path', path_attr_names, value_attr_names),
            ('value', value_attr_names, path_attr_names),
        ):
            getattr(node, f'_invalidate_{group}')()
            for attr in test_names:
                if group == 'path' and frag_is_pointer:
                    # For pointer fragments, the properties are revived before
                    # the end of _invalidate_path(), so this should not raise.
                    assert delattr(node, attr) is None
                else:
                    # hasattr() revives the cached property, so only
                    # by using delattr() can we see if it was already deleted.
                    with pytest.raises(AttributeError):
                        delattr(node, attr)

            assert get_attrs(node) == old_attrs[id(node)]


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
            {"$id": "https://ex.org", "properties": {"foo": {}}},
            JSONPointer('/properties/foo'),
            False,
            URI('https://ex.org#/properties/foo'),
            URI('https://ex.org#/properties/foo'),
            URI('https://ex.org'),
            frozenset(),
        ),
        (
            {"$id": "https://ex.org#", "properties": {"foo": {"$anchor": "bar"}}},
            JSONPointer('/properties/foo'),
            True,
            URI('https://ex.org#bar'),
            URI('https://ex.org#/properties/foo'),
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
    full = JSONSchemaRefId(document)
    r = pointer.evaluate(full)

    assert r.uri == property_uri
    assert r.pointer_uri == base_uri.copy(fragment=pointer.uri_fragment())
    assert r.base_uri == base_uri

    assert register_uri == (property_uri in catalog._schema_cache['default'])
    assert catalog.get_resource(property_uri, cls=JSONSchemaRefId) is r
    for au in additional_uris:
        assert catalog.get_resource(au) is r

def test_non_root_json_pointer_uri():
    # This cannot be covered using JSONSchemaRefId
    base = URI('tag:example.com,2023:base')
    r = PreCalculatePointerUri({'foo': 'bar'}, uri=base)
    assert r.uri == base
    assert r['foo'].uri == base.copy(fragment=r['foo'].path.uri_fragment())


def test_relative_uri_and_default_factory():
    r = JSONResource(
        {},
        uri=URI('foo'),
        default_uri_factory=lambda: URI(f'tag:example.com,2023:/bar/{uuid4()}'),
    )

    assert r.uri == URI('tag:example.com,2023:/bar/foo')


def test_initial_base_uri():
    r = JSONResource(
        {},
        uri=URI('foo'),
        initial_base_uri=URI('tag:example.com,2023:/bar'),
    )
    resolved = URI('tag:example.com,2023:/foo')
    assert r.base_uri == resolved
    assert r.uri == resolved


def test_relative_initial_base_uri():
    r = JSONResource(
        {},
        uri=URI('foo'),
        initial_base_uri=URI('foo/bar'),
        default_uri_factory=lambda: URI(f'tag:example.com,2023:/{uuid4()}'),
    )
    resolved = URI('tag:example.com,2023:/foo/foo')
    assert r.base_uri == resolved
    assert r.uri == resolved


def test_relative_uri_in_non_root():
    base = URI('https://example.com')
    rel = URI('#foo')
    resolved = rel.resolve(base)

    class RelNonRoot(JSONResource):
        def pre_recursion_init(self, *, uri, **kwargs):
            uri = base if self.is_resource_root() else rel
            super().pre_recursion_init(uri=uri, **kwargs)

    r = RelNonRoot({'x': 'y'})
    assert r.uri == base
    assert r['x'].uri == resolved


def test_relative_additional():
    r = JSONResource(
        {},
        uri=URI('https://example.com'),
        additional_uris={URI('/foo')},
    )
    assert r.additional_uris == {
        URI('https://example.com/foo'),
    }


def test_duplicate_uri_error():
    with pytest.raises(DuplicateResourceURIError):
        JSONResource({}, uri=URI(''))


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


def test_change_additional_uris(catalog):
    base_uri_str = "https://example.com/base"
    anchor_str = "a"
    dynamic_anchor_str = "b"

    base_uri = URI(base_uri_str)
    anchor_uri = base_uri.copy(fragment=anchor_str)
    dynamic_anchor_uri = base_uri.copy(fragment=dynamic_anchor_str)
    other_uri = URI("tag:example.com,2023:something-different")

    r = JSONSchemaRefId({
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


def test_invalidate_path():
    base_uri = URI('https://example.com')
    r = JSONSchemaRefId({"$id": str(base_uri), "data": ['a', 'b', 'c']})

    c = r['data'][2]
    assert c.pointer_uri == base_uri.copy(fragment='/data/2')

    del r['data'][1]

    assert r['data'][1] is c
    assert c.pointer_uri == base_uri.copy(fragment='/data/1')


def test_invalidate_path_attr_error():
    r = JSONResource({})

    # First use the cached property so it can be deleted.
    assert r.pointer_uri.fragment == ''
    del r.pointer_uri

    # The test is just that it does not throw.
    assert r._invalidate_path() is None


def test_invalidate_path_not_ready_error():
    r = JSONResource({})
    r._uri = None

    # The test is just that it does not throw.
    assert r._invalidate_path() is None


def test_mixed_document():
    doc = AlternatingResource({'a': {'b': {'c': ['d', 'e']}}})
    node_a = doc['a']
    node_b = doc['a']['b']
    node_c = doc['a']['b']['c']
    node_d = doc['a']['b']['c'][0]
    node_e = doc['a']['b']['c'][1]

    assert type(doc) is AlternatingResource
    assert type(node_a) is NonResourceSpacer
    assert type(node_b) is AlternatingResource
    assert type(node_c) is NonResourceSpacer
    assert type(node_d) is AlternatingResource
    assert type(node_e) is AlternatingResource

    assert doc.is_resource_root()
    assert doc.resource_root is doc
    assert doc.resource_parent is None
    assert doc.parent_in_resource is None
    assert {
        node.uri for node in doc.child_resource_nodes
    } == {node_b.uri}

    assert not node_b.is_resource_root()
    assert node_b.resource_root is doc
    assert node_b.parent is node_a
    assert node_b.resource_parent is doc
    assert node_b.parent_in_resource is doc
    assert {
        node.uri for node in node_b.child_resource_nodes
    } == {node_d.uri, node_e.uri}
    assert node_b.pointer_uri == node_b.base_uri.copy(
        fragment=node_b.path.uri_fragment(),
    )

    for array_node in node_d, node_e:
        assert not array_node.is_resource_root()
        assert array_node.resource_root is doc
        assert array_node.parent is node_c
        assert array_node.resource_parent is node_b
        assert array_node.parent_in_resource is node_b
        assert set(array_node.child_resource_nodes) == set()
        assert array_node.pointer_uri == array_node.base_uri.copy(
            fragment=array_node.path.uri_fragment(),
        )


def test_document_root_not_resource_node():
    with pytest.raises(UnRootedResourceError):
        JSON({"foo": {"bar": 42}}, itemclass=JSONResource)


def test_root_is_not_root():
    class RootAlwaysFalse(JSONResource):
        def is_resource_root(self):
            return False

        @cached_property
        def resource_root(self):
            return self.document_root

    with pytest.raises(InconsistentResourceRootError):
        RootAlwaysFalse({})


def test_false_root_claim():
    class RootAlwaysTrue(JSONResource):
        def is_resource_root(self):
            return True

        @cached_property
        def resource_root(self):
            return self.document_root

    with pytest.raises(InconsistentResourceRootError):
        RootAlwaysTrue({'foo': 'bar'})


def test_parent_resource_types():
    root_base = URI("https://example.com/root")
    embedded_base = URI("tag:example.com,2023:embedded")

    doc = AlternatingResource({
        "$id": str(root_base),
        "properties": {
            "a": {
                "properties": {
                    "b": {
                        "$id": str(embedded_base),
                    },
                },
            },
        },
    })
    node_a = doc['properties']['a']
    node_b = doc['properties']['a']['properties']['b']

    assert doc.resource_parent is None
    assert doc.parent_in_resource is None
    assert node_a.resource_parent is doc
    assert node_a.parent_in_resource is doc
    assert node_b.resource_parent is node_a
    assert node_b.parent_in_resource is None


def test_child_resource_types():
    root_base = URI("https://example.com/root")
    embedded_base = URI("tag:example.com,2023:embedded")
    doc = AlternatingResource({
        "$id": str(root_base),
        "data": {
            "a": {"$anchor": "a"},
            "b": {"$id": str(embedded_base)},
        },
    })
    node_a = doc['data']['a']
    node_b = doc['data']['b']

    cir = list(doc.children_in_resource)
    assert len(cir) == 1
    assert cir[0] is node_a

    crr = list(doc.child_resource_roots)
    assert len(crr) == 1
    assert crr[0] is node_b

    # Test that these functions/properties work for embedded roots
    assert crr[0].is_resource_root()
    assert crr[0].resource_root is crr[0]


@pytest.mark.parametrize('foo,allow_iris', (
    ('foo', False),
    ('foo', True),
    pytest.param('føø', True, marks=pytest.mark.xfail(
        reason='The rfc3986 package is not sufficiently unicode/IRI-aware',
    )),
))
def test_refid_id_plain_name_syntax(foo, allow_iris):
    base = URI('https://example.com')
    foo_uri = base.copy(fragment=foo)
    bar_uri = base.copy(fragment="bar")

    r = JSONSchemaRefId(
        {
            "$id": str(foo_uri),
            "items": {"$id": "#bar"},
        },
        ref_id_keyword_config=RefIdKeywordConfig(
            id_fragment_support='plain-name',
            allow_iris=allow_iris,
        ),
    )

    assert r.base_uri == base
    assert r.uri == base
    assert r.pointer_uri == base.copy(fragment='')
    assert r.additional_uris == {foo_uri}
    assert r['items'].base_uri == base
    assert r['items'].uri == bar_uri
    assert r['items'].pointer_uri == base.copy(fragment='/items')
    assert r['items'].additional_uris == frozenset()


def test_refid_json_pointer_syntax():
    base = URI('https://example.com')
    items_uri = base.copy(fragment="/items")

    r = JSONSchemaRefId(
        {
            "$id": str(base),
            "items": {"$id": "#/items"},
        },
        ref_id_keyword_config=RefIdKeywordConfig(
            id_fragment_support='any',
        ),
    )

    assert r.base_uri == base
    assert r.uri == base
    assert r.pointer_uri == base.copy(fragment='')
    assert r.additional_uris == frozenset()
    assert r['items'].base_uri == base
    assert r['items'].uri == items_uri
    assert r['items'].pointer_uri == items_uri
    assert r['items'].additional_uris == frozenset()


@pytest.mark.parametrize('id_str,frag_support,allow_iris,error', (
    ('about:blank#', 'none', False, 'must not have a fragment'),
    ('about:blank#foo', 'empty', False, 'must not have a non-empty fragment'),
    ('about:blank#føø', 'plain-name', False, 'must be ascii and match'),
    ('about:blank#/føø', 'plain-name', True, 'must match'),
))
def test_refid_id_frag_errors(id_str, frag_support, allow_iris, error):
    with pytest.raises(ValueError, match=error):
        JSONSchemaRefId(
            {"$id": id_str},
            ref_id_keyword_config=RefIdKeywordConfig(
                id_fragment_support=frag_support,
                allow_iris=allow_iris,
            )
        )


def test_refid_set_id():
    r = JSONSchemaRefId({
        "$id": "https://ex.org/foo",
        "items": {
            "$anchor": "baz",
        }
    })
    assert r.keyword_identifiers['$id'] == URI('https://ex.org/foo')

    bar_uri = URI("https://ex.org/bar")
    r["$id"] = str(bar_uri)

    assert r.uri == bar_uri
    assert r.base_uri == bar_uri
    assert r.pointer_uri == bar_uri.copy(fragment='')
    assert r.additional_uris == frozenset()
    
    items = r['items']
    assert items.uri == bar_uri.copy(fragment='baz')
    assert items.base_uri == bar_uri
    assert items.pointer_uri == bar_uri.copy(fragment='/items')
    assert items.additional_uris == frozenset()


@pytest.mark.parametrize('kw', ('$anchor', '$dynamicAnchor'))
def test_refid_set_anchor():
    base = URI('https://ex.org')
    foo_uri = base.copy(fragment='foo')
    bar_uri = base.copy(fragment='bar')
    baz_uri = base.copy(fragment='baz')
    qux_uri = base.copy(fragment='qux')

    r = JSONSchemaRefId({
        "$id": str(base),
        kw: "foo",
        "items": {
            kw: "bar",
        },
    })
    r[kw] = "baz"
    r['items'][kw] = "qux"

    assert r.uri == base
    assert r.base_uri == base
    assert r.pointer_uri == base.copy(fragment='')
    assert r.additional_uris == {baz_uri}

    items = r['items']
    assert items.base_uri == base
    assert items.uri == qux_uri
    assert items.pointer_uri == base.copy(fragment='/items')
    assert r.additional_uris == frozenset()
