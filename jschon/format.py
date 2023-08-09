from functools import cached_property
from typing import Protocol
from jschon import JSON
from jschon.resource import JSONResource

class EvaluableJSONResult(Protocol):
    @contextmanager
    def __call__(
        self,
        instance: JSON,
        schema: EvaluableJSON = None,  # Named for historical reasons
        *,
        cls: Type[EvaluableJSONResult],
    ) -> ContextManager[EvaluableJSONResult]:
        ...

    @property
    def valid(self) -> bool:
        ...

    def annotate(self, value: JSONCompatible) -> None:
        ...

    def fail(self, error: JSONCompatible) -> None:
        ...

    def output(self, format: str, **kwargs: Any) -> JSONCompatible:
        ...


class EvaluableJSON(Protocol):
    def evaluate(
        self,
        instance: JSON,
        result: EvaluableJSONResult,
    ) -> EvaluableJSONResult:
        ...

class JSONFormat(JSONResource):
    @property
    def metadocument_uri(self) -> Optional[URI]:
        if self._metadocument_uri is not None:
            return self._metadocument_uri
        if self.parent_in_format is not None:
            return self.parent_in_format.metadocument_uri

    @property
    def metadocument(self) -> EvaluableJSON:
        if (uri := self.metadocument_uri) is None:
            raise JSONFormatError(
                "The format's metadocument URI has not been set",
            )
        return self.catalog.get_metadocument(uri)

    @cached_property
    def parent_in_format(self):
        candidate = None
        node = self
        while next_candidate := node.parent:
            if isinstance(next_candidate, type(self)):
                candidate = next_candidate
            elif isinstance(next_candidate, JSONFormat):
                # We've changed formats, so stop looking.
                break
            # else we continue past a non-format intermediate node
            node = next_candidate
        return candidate

    @cached_property
    def format_root(self):
        candidate = self
        while next_candidate := candidate.parent_in_format:
            if next_candidate.is_format_root():
                return next_candidate
            candidate = next_candidate

        # The format node without a format parent is implicitly
        # the format root even without meeting an explicit condition.
        # TODO: Is there a use case for a "rootless" format?
        return candidate

    @cached_property
    def is_format_root(self):
        return self.parent_in_format is None

    def validate(self) -> Result:
        """Validate the schema against its metaschema."""
        return self.metadocument.evaluate(self)

    def invalidate_format_tree():
        del self.parent_in_format
        del self.format_root
        del self.is_format_root
