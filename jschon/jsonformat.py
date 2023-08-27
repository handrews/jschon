from __future__ import annotations

from contextlib import contextmanager
from functools import cached_property
from typing import Protocol, runtime_checkable

from jschon.json import JSON
from jschon.resource import JSONResource
from jschon.exc import JschonError

__all__ = [
    'EvaluableJSON',
    'EvaluableJSONResult',
    'JSONFormat',
    'JSONFormatError',
    'MetadocumentClassRequiredError',
]


class JSONFormatError(JschonError):
    """Error originating from the jschon.jsonformat module."""


class MetadocumentClassRequiredError(JSONFormatError):
    """Raised when no metadocuemnt class can be determined."""


@runtime_checkable
class EvaluableJSON(Protocol):
    """Protocol for documents that can evaluate another document.

    """
    def initial_validation_result(
        self,
        instance: JSON,
    ) -> EvaluableJSONresult:
        ...

    def evaluate(
        self,
        instance: JSON,
        result: Optional[EvaluableJSONResult] = None,
    ) -> EvaluableJSONResult:
        ...


@runtime_checkable
class EvaluableJSONResult(Protocol):
    """Protocol for evaluation results, which (like documents) form a tree.

    A result has a boolean validity, and optionally arbitrary additional data.
    It starts in a state of being valid, with no data.  It can be made invalid
    using :meth:`fail`, and data can be added with :meth:`annotate`.  In some
    cases a valid result's additional data can become irrelevant, which can
    be indicated using :meth:`discard`.

    The additional result data is reported using :meth:`output`, which takes
    a ``format`` parameter, the values of which are specific to the type
    of evaluation being performed.
    """
    @contextmanager
    def __call__(
        self,
        instance: JSON,
        schema: EvaluableJSON = None,  # Named for historical reasons
        *,
        cls: Type[EvaluableJSONResult],
    ) -> ContextManager[EvaluableJSONResult]:
        ...

    # TODO: Is the `passed` property also necessary for the interface?
    @property
    def valid(self) -> bool:
        """Return the validity of the instance against the schema."""
        ...

    def annotate(self, value: JSONCompatible) -> None:
        """Annotate the result with arbitrary JSON-format data."""
        ...

    def fail(self, error: Optional[JSONCompatible] = None) -> None:
        """Mark the result as invalid, optionally with an error."""
        ...

    def discard(self) -> None:
        """Indicate that the result should be ignored and discarded."""
        ...

    def output(self, format: str, **kwargs: Any) -> JSONCompatible:
        """Return the evaluation result in the specified `format`.

        :param format: A string specific to the type of evaluation indicating
            the output format.  It should be registered with the
            :func:`~jschon.output.output_formatter` decorator.
        :param kwargs: Keyword arguments to pass to the output formatter.
        """
        ...


class JSONFormat(JSONResource):
    _default_metadocument_cls: ClassVar[Optional[Type[EvaluableJSON]]] = None

    def __init__(
        self,
        *args: Any,
        metadocument_uri: Optional[URI] = None, 
        metadocument_cls: Optional[Type[EvaluableJSON]] = None,
        **kwargs,
    ):
        if (i := kwargs.get('itemclass')) is None:
            kwargs['itemclass'] = type(self)

        self._metadocument_uri: Optional[URI] = metadocument_uri

        self._metadocument_cls: Optional[Type[EvaluableJSON]] = metadocument_cls

        super().__init__(*args, **kwargs)

    def pre_recursion_init(self, *args, **kwargs):
        if self._metadocument_cls is None:
            if (p := self.parent_in_format) is not None:
                self._metadocument_cls = p._metadocument_cls
            elif isinstance(self, EvaluableJSON):
                self._metadocument_cls = type(self)
            else:
                self._metadocument_cls = self._default_metadocument_cls

        if self._metadocument_cls is None:
            raise MetadocumentClassRequiredError(
                "The metadocument_cls parameter is required for this "
                "JSONFormat (sub)class.",
            )
        super().pre_recursion_init(*args, **kwargs)

    def get_metadocument_cls(self) -> EvaluableJSON:
        from jschon.vocabulary import Metaschema
        return Metaschema

    @property
    def metadocument_uri(self) -> Optional[URI]:
        """The URI of a document that describes this document."""
        if self._metadocument_uri is not None:
            return self._metadocument_uri
        if self.parent_in_format is not None:
            return self.parent_in_format.metadocument_uri

    @metadocument_uri.setter
    def metadocument_uri(self, value: Optional[URI]) -> None:
        self._metadocument_uri = value

        # TODO: This is overkill, be more granular?
        self._invalidate_value()

    @cached_property
    def metadocument(self) -> EvaluableJSON:
        """A document describing this document and how to process it.

        The specifics of what it means to "process" a format can vary
        considerably, from simply indicating whether it is a valid
        instance of the format, to selecting the specific semantics
        of the document within the range permitted by the format
        (e.g. in JSON Schema, vocabulary selection).
        """
        if (uri := self.metadocument_uri) is None:
            raise JSONFormatError(
                "The format's metadocument URI has not been set",
            )
        return self.catalog.get_metadocument(
            uri,
            meta_cls=self.get_metadocument_cls(),
        )

    @cached_property
    def format_parent(self) -> Optional[JSONFormat]:
        """Returns the nearest ancestor that is a :class:`JSONFormat`.

        This will always be the same :class:`JSONFormat` subclass as
        the invoking node, but may cross a :attr:`format_root`; see
        also :attr:`parent_in_format` to avoid such crossing.

        Note that a node can have a :attr:`parent` without necessarily
        having a :attr:`format_parent`.
        """
        candidate = None
        current = self

        while (candidate := current.parent) is not None:
            if isinstance(candidate, type(self)):
                return candidate
            current = candidate
        return candidate

    @cached_property
    def parent_in_format(self) -> Optional[JSONFormat]:
        """Returns the nearest ancestor resource node in the same format.

        This skips any intervening ancestor nodes that are not instances
        of this same format, and does not cross :attr:`format_root` boundaries.
        It returns ``None`` if this node is a format root.
        """
        if self.is_format_root():
            return None
        return self.format_parent

    @cached_property
    def format_root(self):
        """The most distant ancestor in the same format-document.

        In the case of nesting formats such as JSON Schema, the
        :attr:`format_root` can have a :attr:`format_parent`,
        but never has a :attr:`parent_in_format`.
        """
        candidate = self
        current = self
        while current is not None:
            if candidate.is_format_root():
                return candidate
            candidate = current
            current = current.parent_in_format

        # The format node without a format parent is implicitly
        # the format root even without meeting an explicit condition.
        return candidate

    def is_format_root(self):
        """Indicates this is the root of a format instance within a document.

        A node without a :attr:`format_parent` should always return true.
        """
        return self.format_parent is None

    def validate(self) -> EvaluableJSONResult:
        """Validate the document against its metadocument."""
        # Validation happens automatically in some circumstances,
        # so ensure that references are evaluated first.
        self.metadocument.format_root.resolve_references()
        return self.metadocument.evaluate(
            self,
            self.metadocument.initial_validation_result(self)
        )

    def _invalidate_path(self) -> None:
        """Causes path-dependent cached attributes to be re-calculated."""
        super()._invalidate_path()

        for attr in (
            'format_parent',
            'parent_in_format',
            'format_root',
        ):
            try:
                delattr(self, attr)
            except AttributeError:
                pass

    def _invalidate_value(self) -> None:
        """Causes value-dependent cached attributes to be re-calculated."""
        super()._invalidate_value()

        for attr in (
            'format_root',
            'parent_in_format',
            'metadocument',
        ):
            try:
                delattr(self, attr)
            except AttributeError:
                pass
