import pytest

from jschon import Catalog, JSONPointer, URI
from jschon.resource import (
    JSONResource,
    ResourceError,
    ResourceNotReadyError,
    ResourceURINotSetError,
    RelativeResourceURIError,
)


def test_constructor_defaults():
    input_value = {'foo': ['bar']}
    jr = JSONResource(input_value)

    assert type(jr) is JSONResource
    assert jr.path == JSONPointer('')
    assert jr.parent is None
    assert jr.parent_in_resource is None
    assert jr.resource_root is jr
    assert jr.is_resource_root() is True

    assert type(jr['foo']) is JSONResource
    assert jr['foo'].path == JSONPointer('/foo')
    assert jr['foo'].parent is jr
    assert jr['foo'].parent_in_resource is jr['foo'].parent
    assert jr['foo'].resource_root is jr
    assert jr['foo'].is_resource_root() is False

    assert type(jr['foo'][0]) is JSONResource
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

@pytest.mark.parametrize(
    'input_uri,catalog_uri,property_uri,pointer_uri,base_uri',
    (
        (
            URI('https://ex.org'),
            URI('https://ex.org'),
            URI('https://ex.org'),
            URI('https://ex.org#'),
            URI('https://ex.org'),
        ),
        (
            URI('https://ex.org#'),
            URI('https://ex.org'),
            URI('https://ex.org'),
            URI('https://ex.org#'),
            URI('https://ex.org'),
        ),
        (
            URI('https://ex.org#/$defs/foo'),
            None,
            URI('https://ex.org#/$defs/foo'),
            URI('https://ex.org#/$defs/foo'),
            URI('https://ex.org'),
        ),
        (
            URI('https://ex.org#bar'),
            URI('https://ex.org#bar'),
            URI('https://ex.org#bar'),
            True,
            URI('https://ex.org'),
        ),
    ),
)
def test_uris(input_uri, catalog_uri, property_uri, pointer_uri, base_uri, catalog):
    r = JSONResource({}, uri=input_uri)
    assert r._catalog_uri == catalog_uri
    assert catalog.get_resource(catalog_uri) is r

