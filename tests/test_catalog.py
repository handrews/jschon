import json
import pathlib
import tempfile
import uuid

import pytest

from jschon import (
    Catalog,
    CatalogError,
    URI,
    JSONPointer,
    JSONSchema,
    JSON,
    create_catalog,
    LocalSource,
    RemoteSource,
)
from jschon.catalog import Source

from tests import example_schema, metaschema_uri_2020_12, schema_bundle1, schema_bundle2

json_example = {"foo": "bar"}


@pytest.fixture
def new_catalog() -> Catalog:
    return Catalog(name=str(uuid.uuid4()))


def test_new_catalog(new_catalog):
    assert not new_catalog._uri_sources
    assert not new_catalog._vocabularies
    assert not new_catalog._schema_cache
    assert not new_catalog._enabled_formats


@pytest.fixture
def setup_tmpdir():
    """Create a temp dir hierarchy containing a JSON file.
    
    Yield (tmpdir path, subdir name, file name) and clean up
    afterwards.
    """
    with tempfile.TemporaryDirectory() as tmpdir_path:
        with tempfile.TemporaryDirectory(dir=tmpdir_path) as subdir_path:
            with tempfile.NamedTemporaryFile(dir=subdir_path) as f:
                f.write(json.dumps(json_example).encode())
                f.flush()
                yield tmpdir_path, pathlib.Path(subdir_path).name, pathlib.Path(f.name).name


@pytest.mark.parametrize('base_uri', [
    'http://example.com/',
    'http://example.com/foo/',
    'http://example.com/foo/bar/',
])
def test_local_source(base_uri, setup_tmpdir, new_catalog):
    tmpdir_path, subdir_name, jsonfile_name = setup_tmpdir
    new_catalog.add_uri_source(URI(base_uri), LocalSource(pathlib.Path(tmpdir_path)))
    json_doc = new_catalog.load_json(URI(f'{base_uri}{subdir_name}/{jsonfile_name}'))
    assert json_doc == json_example
    # incorrect base URI
    with pytest.raises(CatalogError):
        new_catalog.load_json(URI(f'http://example.net/{subdir_name}/{jsonfile_name}'))
    # incorrect file name
    with pytest.raises(CatalogError):
        new_catalog.load_json(URI(f'{base_uri}{subdir_name}/baz'))


@pytest.mark.parametrize('base_uri', [
    'http://example.com/',
    'http://example.com/foo/',
    'http://example.com/foo/bar/',
])
def test_remote_source(base_uri, httpserver, new_catalog):
    new_catalog.add_uri_source(URI(base_uri), RemoteSource(URI(httpserver.url_for(''))))
    httpserver.expect_request('/baz/quux').respond_with_json(json_example)
    json_doc = new_catalog.load_json(URI(f'{base_uri}baz/quux'))
    assert json_doc == json_example
    # incorrect base URI
    with pytest.raises(CatalogError):
        new_catalog.load_json(URI('http://example.net/baz/quux'))
    # incorrect path
    with pytest.raises(CatalogError):
        new_catalog.load_json(URI(f'{base_uri}baz/quuz'))


@pytest.mark.parametrize('base_uri', [
    '//example.com/foo/bar/',  # no scheme
    'http://Example.com/foo/bar/',  # not normalized
    'http://example.com/foo/#',  # contains empty fragment
    'http://example.com/foo/#bar',  # contains non-empty fragment
    'http://example.com/foo/bar',  # does not end with '/'
])
def test_uri_source_invalid_uri(base_uri, new_catalog):
    with pytest.raises(CatalogError):
        new_catalog.add_uri_source(URI(base_uri), LocalSource('/'))


@pytest.mark.parametrize('uri', [
    '//example.com/foo/bar/file.json',  # no scheme
    'http://Example.com/foo/bar/file.json',  # not normalized
    'http://example.com/foo/file.json#',  # contains empty fragment
    'http://example.com/foo/file.json#bar',  # contains non-empty fragment
])
def test_load_json_invalid_uri(uri, new_catalog):
    with pytest.raises(CatalogError):
        new_catalog.load_json(URI(uri))


@pytest.mark.parametrize('uri, is_known', [
    ("https://json-schema.org/draft/2020-12/vocab/core", True),
    ("https://json-schema.org/draft/2020-12/vocab/applicator", True),
    ("https://json-schema.org/draft/2020-12/vocab/unevaluated", True),
    ("https://json-schema.org/draft/2020-12/vocab/validation", True),
    ("https://json-schema.org/draft/2020-12/vocab/meta-data", True),
    ("https://json-schema.org/draft/2020-12/vocab/format-annotation", True),
    ("https://json-schema.org/draft/2020-12/meta/format-assertion", False),
    ("https://json-schema.org/draft/2020-12/vocab/content", True),
])
def test_get_vocabulary(uri, is_known, catalog):
    if is_known:
        vocabulary = catalog.get_vocabulary(URI(uri))
        assert vocabulary.uri == uri
    else:
        with pytest.raises(CatalogError):
            catalog.get_vocabulary(URI(uri))


@pytest.fixture
def example_schema_uri():
    schema = JSONSchema(example_schema)
    return schema.uri


@pytest.mark.parametrize('ptr, is_schema', [
    ("", True),
    ("/$id", False),
    ("/$defs", False),
    ("/if", True),
    ("/then", True),
    ("/else", True),
])
def test_get_schema(example_schema_uri, ptr, is_schema, catalog):
    uri = example_schema_uri.copy(fragment=ptr)
    if is_schema:
        subschema = catalog.get_schema(uri)
        assert JSONPointer(ptr).evaluate(example_schema) == subschema
    else:
        with pytest.raises(CatalogError):
            catalog.get_schema(uri)


@pytest.fixture
def dict_source():
    class DictSource(Source):
        schemas =  {
            'bundle1': schema_bundle1,
            'bundle2': schema_bundle2,
            'control': {
                '$schema': 'https://json-schema.org/draft/2020-12/schema',
                '$ref': 'control-target',
            },
        }
        def __call__(self, relative_path):
            return self.schemas[relative_path]

    return DictSource


@pytest.fixture
def deferred_catalog(dict_source):
    deferred_catalog = create_catalog(
        '2019-09',
        '2020-12',
        name='deferred',
        resolve_references=False,
    )
    deferred_catalog.add_uri_source(URI('https://example.com/'), dict_source())

    return deferred_catalog


@pytest.fixture
def bundle(deferred_catalog):
    bundle1 = deferred_catalog.get_schema(URI('https://example.com/bundle1'), cacheid='test')
    bundle2 = deferred_catalog.get_schema(URI('https://example.com/bundle2'), cacheid='test')
    control = deferred_catalog.get_schema(URI('https://example.com/control'))

    return deferred_catalog, bundle1, bundle2, control


def assert_resolved_catalog(deferred_catalog, bundle1, bundle2, control):
    assert bundle1.references_resolved is True
    assert bundle2.references_resolved is True
    assert control.references_resolved is False

    with pytest.raises(CatalogError, match=control['$ref'].value):
        deferred_catalog.resolve_references()


def test_deferred_ref_resolution(bundle):
    deferred_catalog, bundle1, bundle2, control = bundle

    assert bundle1.references_resolved is False
    assert bundle2.references_resolved is False
    assert control.references_resolved is False

    deferred_catalog.resolve_references(cacheid='test')
    assert_resolved_catalog(*bundle)


def test_auto_resolution(bundle):
    deferred_catalog, bundle1, bundle2, control = bundle

    assert bundle1.evaluate(JSON([])).valid is True
    assert_resolved_catalog(*bundle)


def test_resolution_that_loads_unresolved(deferred_catalog, dict_source):
    control = deferred_catalog.get_schema(URI('https://example.com/control'))
    assert control.references_resolved is False

    # Allow resolution of the unresolved reference in control
    target_uri = URI('https://example.com/control-target')
    dict_source.schemas['control-target'] = {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        '$id': str(target_uri),
    }
    assert target_uri not in deferred_catalog._schema_cache['default']

    deferred_catalog.resolve_references()

    assert control.references_resolved is True

    assert target_uri in deferred_catalog._schema_cache['default']
    target = deferred_catalog.get_schema(target_uri)
    assert target.references_resolved is True


def cached_schema(uri, schema, cacheid):
    kwargs = {'uri': uri, 'metaschema_uri': metaschema_uri_2020_12}
    if cacheid is not None:
        kwargs['cacheid'] = cacheid
    return JSONSchema(schema, **kwargs)


def test_cache_independence(catalog):
    uri = URI("http://example.com")
    cached_schema(uri, {"const": 0}, None)  # 'default' cache
    cached_schema(uri, {"const": 1}, 'one')
    cached_schema(uri, {"const": 2}, 'two')
    assert catalog.get_schema(uri)["const"] == 0
    assert catalog.get_schema(uri, cacheid='default')["const"] == 0
    assert catalog.get_schema(uri, cacheid='one')["const"] == 1
    assert catalog.get_schema(uri, cacheid='two')["const"] == 2


def test_metaschema_isolation():
    new_catalog = create_catalog('2019-09', '2020-12', name=str(uuid.uuid4()))
    assert new_catalog._schema_cache.keys() == {'__meta__'}

    # mask the metaschema with a boolean false schema, in the fubar cache
    cached_schema(metaschema_uri_2020_12, False, 'fubar')
    uri = URI("http://example.com")
    fubar_schema = cached_schema(uri, {"$ref": str(metaschema_uri_2020_12)}, 'fubar')
    assert fubar_schema.evaluate(JSON(True)).valid is False

    # masking the metaschema has no impact on other caches
    okay_schema = cached_schema(uri, {"$ref": str(metaschema_uri_2020_12)}, 'okay')
    assert okay_schema.evaluate(JSON(True)).valid is True
    okay_schema = cached_schema(uri, {"$ref": str(metaschema_uri_2020_12)}, None)
    assert okay_schema.evaluate(JSON(True)).valid is True
