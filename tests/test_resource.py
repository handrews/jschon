from jschon import Catalog
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
    # assert jr.path == JSONPointer('')
    # assert jr.parent is None
    # assert jr.parent_in_resource is None
    # assert jr.resource_root is jr
    # assert jr.is_resource_root() is True

    # assert type(jr['foo']) is JSONResource
    # assert jr['foo'].path == JSONPointer('/foo')
    # assert jr['foo'].parent is jr
    # assert jr['foo'].parent_in_resource is jr
    # assert jr['foo'].resource_root is jr
    # assert jr['foo'].is_resource_root() is False

    # assert type(jr['foo'][0]) is JSONResource
    # assert jr['foo'][0].path == JSONPointer('/foo/0')
    # assert jr['foo'][0].parent is jr
    # assert jr['foo'][0].parent_in_resource is jr
    # assert jr['foo'][0].resource_root is jr
    # assert jr['foo'][0].is_resource_root() is False

    # assert jr.catalog == Catalog.get_catalog()
    # assert jr.cacheid == 'default'
    # assert jr.references_resovled is True
    # assert jr.value == input_value

    # assert jr.uri.scheme == 'urn'
    # assert jr.uri.path.startswith('uuid:')
    # assert jr.uri.query is None
    # assert jr.uri.fragment is None
    # assert jr.ptr_uri.fragment == ''
    # assert jr.ptr_uri.copy(fragment=None) == jr.uri
    # assert jr.base_uri == jr.uri
