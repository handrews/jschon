import json
import pathlib

import pytest
import rfc3986

from jschon.catalogue import Catalogue
from jschon.json import JSON
from jschon.jsonschema import JSONSchema, evaluate
from tests import metaschema_uri

Catalogue.add_local(
    base_uri=rfc3986.uri_reference('http://localhost:1234/'),
    base_dir=pathlib.Path(__file__).parent / 'jsonschema_testsuite' / 'remotes',
)


def pytest_generate_tests(metafunc):
    argnames = ('schema', 'data', 'valid')
    argvalues = []
    testids = []
    testsuite_dir = pathlib.Path('jsonschema_testsuite/tests/draft2019-09')
    testfile_paths = sorted(testsuite_dir.rglob('*.json'))
    for testfile_path in testfile_paths:
        with testfile_path.open() as testfile:
            testcases = json.load(testfile)
            for testcase in testcases:
                for test in testcase['tests']:
                    argvalues.append(pytest.param(testcase['schema'], test['data'], test['valid']))
                    testids.append(f"{testfile_path.name} -> {testcase['description']} -> {test['description']}")
    metafunc.parametrize(argnames, argvalues, ids=testids)


def test_validate(schema, data, valid):
    s = JSONSchema(schema, metaschema_uri=metaschema_uri)
    assert s.keywords.keys() == schema.keys() if isinstance(schema, dict) else not s.keywords
    instance = evaluate(s, JSON(data))
    assert instance.valid == valid
