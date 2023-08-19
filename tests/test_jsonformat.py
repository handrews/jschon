import pytest

from jschon import URI
from jschon.jsonformat import (
    EvaluableJSONResult,
    JSONFormat,
    JSONFormatError,
    EvaluableJSON,
    MetadocumentClassRequiredError,
)


class EvaluatorResult(EvaluableJSONResult):
    def __init__(self, evaluator, instance, *, validating_with=None, **kwargs):
        self.evaluator = evaluator
        self.instance = instance
        self.validating_with = validating_with

    def fail(self):
        self._valid = False

    def annotate(self, value):
        self._annotation = value

    def discard(self):
        self._discard = True

    def output(self, format, **kwargs):
        return self._annotation


class Evaluator(JSONFormat, EvaluableJSON):
    def __init__(self, *args, **kwargs):
        self._valid = True
        self._discard = False
        self._annotation = None
        super().__init__(*args, **kwargs)

    def initial_validation_result(self, instance):
        return EvaluatorResult(self, instance, validating_with=self)

    def evaluate(self, instance, result=None):
        return result or self.initial_validation_result(instance)
        self._result = result


class Evaluator2(Evaluator):
    pass


class JSONFormat2(JSONFormat):
    _default_metadocument_cls = Evaluator2


def test_default_constructor():
    f = JSONFormat({"a": []}, metadocument_cls=Evaluator)

    for node in f, f['a']:
        assert type(node) is JSONFormat
        assert node.metadocument_uri is None
        assert node._metadocument_cls is Evaluator

        with pytest.raises(JSONFormatError):
            node.metadocument

    assert f.format_parent is None
    assert f.parent_in_format is None
    assert f.is_format_root() is True
    assert f.format_root is f

    assert f['a'].format_parent is f
    assert f['a'].parent_in_format is f
    assert f['a'].is_format_root() is False
    assert f['a'].format_root is f


def test_with_metadocument_uri(catalog):
    md_uri = URI('https://example.org/meta')
    f = JSONFormat(
        {"a": []},
        metadocument_uri=md_uri,
        metadocument_cls=Evaluator,
    )

    md = Evaluator({}, uri=md_uri, cacheid='__meta__')
    assert md.uri == md_uri
    assert md_uri in catalog._schema_cache['__meta__']

    for node in f, f['a']:
        assert type(node) is JSONFormat
        assert node.metadocument_uri is md_uri
        assert node.metadocument is md


def test_itemclass():
    f = JSONFormat(
        {"a": []},
        itemclass=JSONFormat2,
        metadocument_cls=Evaluator,
    )

    a = f['a']
    assert type(a) is JSONFormat2
    assert a._metadocument_cls is Evaluator2
    assert a.format_parent is None
    assert a.parent_in_format is None
    assert a.is_format_root() is True
    assert a.format_root is a


def test_subclass_in_format():
    f = JSONFormat2(
        {"a": []},
        itemclass=JSONFormat,
    )

    a = f['a']
    assert type(a) is JSONFormat
    assert a._metadocument_cls is Evaluator2
    assert a.format_parent is f
    assert a.parent_in_format is f
    assert a.is_format_root() is False
    assert a.format_root is f


def test_format_cls_required():
    with pytest.raises(MetadocumentClassRequiredError):
        JSONFormat({})
