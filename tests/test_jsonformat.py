import pytest

from jschon import URI, JSONPointer
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


@pytest.fixture
def metadocument():
    md_uri = URI('https://example.org/meta')
    md = Evaluator({}, uri=md_uri, cacheid='__meta__')
    return md


def test_format_cls_required():
    with pytest.raises(MetadocumentClassRequiredError):
        JSONFormat({})


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


def test_with_metadocument(metadocument):
    f = JSONFormat(
        {"a": []},
        metadocument_uri=metadocument.uri,
        metadocument_cls=Evaluator,
    )

    for node in f, f['a']:
        assert type(node) is JSONFormat
        assert node.metadocument_uri is metadocument.uri
        assert node.metadocument is metadocument


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


def test_validate(metadocument):
    f = JSONFormat(
        {},
        metadocument_uri=metadocument.uri,
        metadocument_cls=type(metadocument),
    )
    r = f.validate()
    assert r.instance == f
    assert r.evaluator == metadocument
    assert r.validating_with == metadocument 


class ZeroIsRoot(JSONFormat):
    _default_metadocument_cls = Evaluator
    _invalidated = False

    def is_format_root(self):
        # Deleting an array element triggers an _invalidate_path() call
        return self.format_parent is None or (
            self.parent.type == 'array' and self.key == '0'
        )

    def _invalidate_path(self):
        super()._invalidate_path()
        self._invalidated = True


@pytest.mark.parametrize('path', (JSONPointer('/a/1'), JSONPointer('/a/1/c')))
def test_invalidate_path(path):

    f = JSONFormat({}, metadocument_cls=Evaluator)
    assert f.format_parent is None
    assert f.parent_in_format is None
    assert f.format_root is f

    f._invalidate_path()

    # Ensure 2nd call does not cause an AttributeError (for branch coverage)
    assert f._invalidate_path() is None

    for attr in ('format_parent', 'parent_in_format', 'format_root'):
        with pytest.raises(AttributeError):
            delattr(f, attr)
