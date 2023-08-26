from __future__ import annotations

import pathlib
import uuid
from contextlib import contextmanager
from importlib import import_module
from os import PathLike
from typing import Any, ContextManager, Dict, Hashable, Set, Union

from jschon.exceptions import CatalogError, JSONPointerError, URIError
from jschon.json import JSONCompatible
from jschon.jsonpointer import JSONPointer
from jschon.resource import JSONResource
from jschon.jsonformat import JSONFormat, EvaluableJSON
from jschon.jsonschema import JSONSchema
from jschon.uri import URI
from jschon.utils import json_loadf, json_loadr
from jschon.vocabulary import KeywordClass, Metaschema, Vocabulary

__all__ = [
    'Catalog',
    'Source',
    'LocalSource',
    'RemoteSource',
]


class Source:
    def __init__(self, suffix: str = None) -> None:
        self.suffix = suffix

    def __call__(self, relative_path: str) -> JSONCompatible:
        raise NotImplementedError


class LocalSource(Source):
    def __init__(self, base_dir: Union[str, PathLike], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_dir = base_dir

    def __call__(self, relative_path: str) -> JSONCompatible:
        filepath = pathlib.Path(self.base_dir) / relative_path
        if self.suffix:
            filepath = str(filepath)
            filepath += self.suffix

        try:
            return json_loadf(filepath)
        except OSError as e:
            if e.filename is not None:
                # The filename for OSError is not included in
                # the exception args, which is what the Catalog
                # puts in the CatalogError.  So it needs to be
                # added separately for filesystem errors.
                raise CatalogError(f'{e.strerror}: {e.filename!r}')
            raise


class RemoteSource(Source):
    def __init__(self, base_url: URI, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_url = base_url

    def __call__(self, relative_path: str) -> JSONCompatible:
        url = str(URI(relative_path).resolve(self.base_url))
        if self.suffix:
            url += self.suffix

        return json_loadr(url)


class Catalog:
    """The :class:`Catalog` acts as a schema cache, enabling schemas and
    subschemas to be indexed, re-used, and cross-referenced by URI."""

    _catalog_registry: Dict[Hashable, Catalog] = {}

    @classmethod
    def get_catalog(cls, name: str = 'catalog') -> Catalog:
        try:
            return cls._catalog_registry[name]
        except KeyError:
            raise CatalogError(f'Catalog name "{name}" not found.')

    def __init__(
        self,
        name: str = 'catalog',
        *,
        resolve_references: bool = True,
    ) -> None:
        """Initialize a :class:`Catalog` instance.

        :param name: a unique name for this :class:`Catalog` instance
        :param resolve_references: passed through to any
            :class:`~jschon.jsonschema.JSONSChema` constructor calls
        """
        self.__class__._catalog_registry[name] = self

        self.name: str = name
        """The unique name of this :class:`Catalog` instance."""

        self._uri_sources: Dict[str, Source] = {}
        self._vocabularies: Dict[URI, Vocabulary] = {}
        self._schema_cache: Dict[Hashable, Dict[URI, JSONSchema]] = {}
        self._enabled_formats: Set[str] = set()
        self._auto_resolve_references: bool = resolve_references

    def __repr__(self) -> str:
        """Return `repr(self)`."""
        return f'{self.__class__.__name__}({self.name!r})'

    def add_uri_source(self, base_uri: Union[URI, None], source: Source) -> None:
        """Register a source for loading URI-identified JSON resources.

        A base URI of ``None`` registers a default source that handles any
        URI that does not match any registered base URI string.

        :param base_uri: a normalized, absolute URI - including scheme, without
            a fragment, and ending with ``'/'`` or None to match complete URIs
        :param source: a :class:`Source` object
        :raise CatalogError: if `base_uri` is invalid
        """
        if base_uri is None:
            prefix = ''
        else:
            try:
                base_uri.validate(
                    require_scheme=True,
                    require_normalized=True,
                    allow_fragment=False,
                )
            except URIError as e:
                raise CatalogError from e

            if not base_uri.path or not base_uri.path.endswith('/'):
                raise CatalogError('base_uri must end with "/"')
            prefix = str(base_uri)

        self._uri_sources[prefix] = source

    def load_json(self, uri: URI) -> JSONCompatible:
        """Load a JSON-compatible object from the source for `uri`.

        If there are multiple candidate base URIs for `uri`, the most specific
        match (i.e. the longest one) is selected.

        :param uri: a normalized, absolute URI - including scheme, without
            a fragment
        :raise CatalogError: if `uri` is invalid, a source is not available
            for `uri`, or if a loading error occurs
        """
        try:
            uri.validate(require_scheme=True, require_normalized=True, allow_fragment=False)
        except URIError as e:
            raise CatalogError from e

        uristr = str(uri)
        candidates = [
            (prefix, source)
            for prefix, source in self._uri_sources.items()
            if uristr.startswith(prefix)
        ]
        if candidates:
            candidates.sort(key=lambda c: len(c[0]), reverse=True)
            prefix, source = candidates[0]
            relative_path = uristr[len(prefix):]
            try:
                return source(relative_path)
            except CatalogError:
                raise
            except Exception as e:
                raise CatalogError(*e.args) from e

        raise CatalogError(f'A source is not available for "{uri}"')

    def create_vocabulary(self, uri: URI, *kwclasses: KeywordClass) -> Vocabulary:
        """Create a :class:`~jschon.vocabulary.Vocabulary` object, which
        may be used by a :class:`~jschon.vocabulary.Metaschema` to provide
        keyword classes used in schema construction.

        :param uri: the URI identifying the vocabulary
        :param kwclasses: the :class:`~jschon.vocabulary.Keyword` classes
            constituting the vocabulary

        :returns: the newly created :class:`~jschon.vocabulary.Vocabulary` instance
        """
        self._vocabularies[uri] = Vocabulary(uri, *kwclasses)
        return self._vocabularies[uri]

    def get_vocabulary(self, uri: URI) -> Vocabulary:
        """Get a :class:`~jschon.vocabulary.Vocabulary` by its `uri`.

        :param uri: the URI identifying the vocabulary
        :raise CatalogError: if `uri` is not a recognized vocabulary URI
        """
        try:
            return self._vocabularies[uri]
        except KeyError:
            raise CatalogError(f"Unrecognized vocabulary URI '{uri}'")

    def create_metadocument(
        self,
        uri: URI,
        *meta_args,
        meta_cls: Type[EvaluableJSON] = Metaschema,
        **meta_kwargs: Any,
    ) -> EvaluableJSON:
        metadocument_doc = self.load_json(uri.copy(fragment=None))
        metadocument = meta_cls(
            self,
            metadocument_doc,
            *meta_args,
            **meta_kwargs,
            uri=uri,
        )
        if not self._auto_resolve_references:
            self.resolve_references(cacheid='__meta__')
        if not metadocument.validate().valid:
            raise CatalogError(
                "The metadocument is invalid against its own metadocument "
                f'"{metadocument_doc["$schema"]}"'
            )
        return metadocument

    def create_metaschema(
            self,
            uri: URI,
            default_core_vocabulary_uri: URI = None,
            *default_vocabulary_uris: URI,
            **kwargs: Any,
    ) -> Metaschema:
        """Create, cache and validate a :class:`~jschon.vocabulary.Metaschema`.

        :param uri: the URI identifying the metaschema
        :param default_core_vocabulary_uri: the URI identifying the metaschema's
            core :class:`~jschon.vocabulary.Vocabulary`, used in the absence
            of a ``"$vocabulary"`` keyword in the metaschema JSON file, or
            if a known core vocabulary is not present under ``"$vocabulary"``
        :param default_vocabulary_uris: default :class:`~jschon.vocabulary.Vocabulary`
            URIs, used in the absence of a ``"$vocabulary"`` keyword in the
            metaschema JSON file
        :param kwargs: additional keyword arguments to pass through to the
            :class:`~jschon.jsonschema.JSONSchema` constructor

        :returns: the newly created :class:`~jschon.vocabulary.Metaschema` instance

        :raise CatalogError: if the metaschema is not valid
        """
        metaschema_doc = self.load_json(uri)

        default_core_vocabulary = (
            self.get_vocabulary(default_core_vocabulary_uri)
            if default_core_vocabulary_uri
            else None
        )
        default_vocabularies = [
            self.get_vocabulary(vocab_uri)
            for vocab_uri in default_vocabulary_uris
        ]

        try:
            return self.create_metadocument(
                uri,
                default_core_vocabulary,
                *default_vocabularies,
                **kwargs,
                meta_cls=Metaschema,
            )
        except CatalogError as e:
            raise CatalogError(
                "The metaschema is invalid against its own metaschema "
                f'"{metaschema_doc["$schema"]}"'
            ) from e

    def get_metadocument(
        self,
        uri: URI,
        meta_cls: Type[EvaluableJSON] = EvaluableJSON,
    ) -> EvaluableJSON:
        try:
            metadocument = self._schema_cache['__meta__'][uri]
        except KeyError:
            metadocument = None

        if metadocument is None:
            metadocument = self.create_metadocument(uri, meta_cls=meta_cls)

        if not isinstance(metadocument, EvaluableJSON):
            raise CatalogError(f"The schema referenced by {uri} is not an evaluable metadocument")

        return metadocument

    def get_metaschema(self, uri: URI) -> Metaschema:
        """Get a metaschema identified by `uri` from the ``'__meta__'`` cache, or
        load it from configured sources if not already cached.

        Note that metaschemas that do not declare a known core vocabulary
        in ``"$vocabulary"`` must first be created using :meth:`create_metaschema`.

        :param uri: the URI identifying the metaschema

        :raise CatalogError: if the object referenced by `uri` is not
            a :class:`~jschon.vocabulary.Metaschema`, or if it is not valid
        :raise JSONSchemaError: if the metaschema is loaded from sources
            but no known core vocabulary is present in ``"$vocabulary"``
        """
        try:
            metaschema = self._schema_cache['__meta__'][uri]
        except KeyError:
            metaschema = None

        if not metaschema:
            metaschema = self.create_metaschema(uri)

        if not isinstance(metaschema, Metaschema):
            raise CatalogError(f"The schema referenced by {uri} is not a metaschema")

        return metaschema


    def enable_formats(self, *format_attr: str) -> None:
        """Enable validation of the specified format attributes.

        These may include formats defined in :mod:`jschon.formats`
        and elsewhere.
        """
        import_module('jschon.formats')
        self._enabled_formats |= set(format_attr)

    def is_format_enabled(self, format_attr) -> bool:
        """Return True if validation is enabled for `format_attr`,
        False otherwise."""
        return format_attr in self._enabled_formats

    def add_resource(
        self,
        uri: URI,
        resource: JSONResource,
        *,
        cacheid: Hashable = 'default',
    ) -> None:
        """Add a (sub)resource to a cache.

        Note that this method is called automatically during resource construction.

        :param uri: the URI identifying the (sub)resource
        :param schema: the :class:`~jschon.resource.JSONResource` instance to cache
        :param cacheid: schema cache identifier
        """
        self._schema_cache.setdefault(cacheid, {})
        self._schema_cache[cacheid][uri] = resource

    def add_schema(
            self,
            uri: URI,
            schema: JSONSchema,
            *,
            cacheid: Hashable = 'default',
    ) -> None:
        """Add a (sub)schema to a cache.

        Note that this method is called automatically during schema construction.

        :param uri: the URI identifying the (sub)schema
        :param schema: the :class:`~jschon.jsonschema.JSONSchema` instance to cache
        :param cacheid: schema cache identifier
        """
        self.add_resource(uri, schema, cacheid=cacheid)

    def del_resource(
        self,
        uri: URI,
        *,
        cacheid: Hashable = 'default'
    ) -> None:
        """Remove a (sub)resource from a cache.

        :param uri: the URI identifying the (sub)schema
        :param cacheid: schema cache identifier
        """
        if cacheid in self._schema_cache:
            self._schema_cache[cacheid].pop(uri, None)

    def del_schema(
        self,
        uri: URI,
        *,
        cacheid: Hashable = 'default',
    ) -> None:
        """Remove a (sub)schema from a cache.

        :param uri: the URI identifying the (sub)schema
        :param cacheid: schema cache identifier
        """
        self.del_resource(uri, cacheid=cacheid)

    def get_resource(
        self,
        uri: URI,
        *,
        metadocument_uri: URI = None,
        cacheid: Hashable = 'default',
        cls: Union[Type[JSONResource], Type[JSONSchema]] = JSONResource,
        factory: Optional[Callable[[...], JSONResource]] = None,
        fragment: bool = False,
    ) -> JSONResource:
        """Get a (sub)resource identified by `uri` from a cache, or
        load it from a :class:`Source` if not already cached.

        :param uri: the URI identifying the (sub)resource
        :param metadocument_uri: passed to the resource constrructor when
            loading a new instance from a source; currently this is only
            used with :class:`JSONSchema` instances where it is passed
            as the ``metaschema_uri`` parameter
        :param cacheid: schema cache identifier
        :param cls: The :class:`jschon.resource.JSONResource` subclass to
            instantiate
        :param factory: A callable that will instantiate the correct subclass
            in place of invoking the ``cls`` parameter directly; the result
            will be type-checked against ``cls``
        :param fragment: If true, a ``request_uri`` parameter will be passed
            to the ``factory`` or ``cls`` invocation which will include
            the requested URI with its fragment, if any
        :raise CatalogError: if a schema cannot be found for `uri`, or if the
            object referenced by `uri` does not match the type in the
            ``cls`` parameter
        """
        try:
            return self._schema_cache[cacheid][uri]
        except KeyError:
            pass

        resource = None
        base_uri = uri.copy(fragment=False)
        if factory is None:
            factory = cls

        if uri.fragment is not None:
            try:
                resource = self._schema_cache[cacheid][base_uri]
            except KeyError:
                pass

        if resource is None:
            doc = self.load_json(base_uri)
            kwargs = {}
            if issubclass(cls, JSONSchema):
                kwargs['metaschema_uri'] = metadocument_uri
            if fragment:
                kwargs['request_uri'] = uri
            resource = factory(
                doc,
                catalog=self,
                cacheid=cacheid,
                uri=base_uri,
                resolve_references=self._auto_resolve_references,
                **kwargs,
            )
            try:
                return self._schema_cache[cacheid][uri]
            except KeyError:
                pass

        if uri.fragment:
            try:
                ptr = JSONPointer.parse_uri_fragment(uri.fragment)
                resource = ptr.evaluate(resource)
            except JSONPointerError as e:
                raise CatalogError(f"Schema not found for {uri}") from e

        if not isinstance(resource, cls):
            raise CatalogError(
                f"The object referenced by {uri} is not an instance of {cls.__name__}; "
                f"it is an instance of {type(resource).__name__}",
            )

        return resource

    def get_schema(
        self,
        uri: URI,
        *,
        metaschema_uri: URI = None,
        cacheid: Hashable = 'default',
        cls: Type[JSONSchema] = JSONSchema,
        factory: Optional[Callable[[...], JSONSchema]] = None,
    ) -> JSONSchema:
        """Get a (sub)schema identified by `uri` from a cache, or
        load it from disk if not already cached.

        :param uri: the URI identifying the (sub)schema
        :param metaschema_uri: passed to the :class:`~jschon.jsonschema.JSONSchema`
            constructor when loading a new instance from disk
        :param cacheid: schema cache identifier
        :raise CatalogError: if a schema cannot be found for `uri`, or if the
            object referenced by `uri` is not a :class:`~jschon.jsonschema.JSONSchema`
        """
        return self.get_resource(
            uri,
            metadocument_uri=metaschema_uri,
            cacheid=cacheid,
            cls=cls,
            factory=factory,
        )

    def resolve_references(self, cacheid: Hashable = 'default') -> None:
        """Ensures that all references in all schemas in a cache have been resolved.

        This method is a convenience method for use after instantiatng numerous schemas
        with ``resolve_references=False``.  It ensures that reference resolution will
        not fail during :meth:`~jschon.jsonschema.JSONSchema.evaluate` by calling
        :meth:`~jschon.jsonschema.JSONSchema.resolve_references` on each schema.

        :param cacheid: The cache in which to resolve all schema references.
        """
        # Note that self._auto_resolve_references is irrelevant as JSONSchema
        # instances can be independently instantiated without resolving references.

        # Resolving references can load additional schemas, so we need to iterate
        # over a frozen copy and keep re-checking the cache.
        cache = self._schema_cache[cacheid]
        cache_keys = frozenset(cache.keys())
        resolved = set()
        while len(resolved) < len(cache.keys()):
            for schema_uri in cache_keys:
                cache[schema_uri].resolve_references()
                resolved.add(schema_uri)
            cache_keys = cache.keys() - resolved

    @contextmanager
    def cache(self, cacheid: Hashable = None) -> ContextManager[Hashable]:
        """Context manager for a schema cache.

        Example usage::

            with catalog.cache() as cacheid:
                schema = JSONSchema(..., cacheid=cacheid)

        The cache and its contents are popped from the catalog
        upon exiting the ``with`` block.
        """
        if cacheid is None:
            cacheid = uuid.uuid4()

        if cacheid in self._schema_cache:
            raise CatalogError("cache identifier is already in use")

        try:
            yield cacheid
        finally:
            self._schema_cache.pop(cacheid, None)
