from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from typing import Any, ContextManager, Dict, Hashable, Iterator, Mapping, Optional, TYPE_CHECKING, Tuple, Type, Union

from jschon import JSON, URI
from jschon.exc import JschonError

if TYPE_CHECKING:
    from jschon.catalog import Catalog, Source

__all__ = [
    'JSONResource',
    'ResourceError',
    'ResourceNotReadyError',
    'ResourceURINotSetError',
    'RelativeResourceURIError',
]


class ResourceError(JschonError):
    """An error originating in the :mod:`~jschon.resource` module."""


class ResourceNotReadyError(ResourceError):
    """Raised if the ``path``, ``parent``, and ``resource_root`` attributes are not availalbe on resource initialization."""


class ResourceURINotSetError(ResourceError):
    """Raised when accessing any URI property before the URI has been set."""

class RelativeResourceURIError(ResourceError):
    """Raised when attempting to set a URI to a relative URI-reference without an available base URI."""


@dataclass(frozen=True)
class ResourceURIs:
    """Data structure for organizing URIs by use case."""

    catalog_uri: Optional[URI]
    """The URI to use as a :class:`~jschon.catalog.Catalog key, if any."""

    property_uri: URI
    """The URI to return from the :attr:`uri` property."""

    pointer_uri: URI
    """The URI consisting of the base URI plus a JSON Pointer fragment."""

    base_uri: URI
    """The base URI for the resource."""


class JSONResource(JSON):
    """Supports URIs and :class:`~jschon.catalog.Catalog` management.

    This class faciliates the extension of ``jschon`` to formats
    beyond JSON Schema, and is not intended for direct use.

    Resources have at least one URI and can serve as reference sources
    and / or targets.  They can be loaded by requesting them by URI
    from a :class:`~jschon.catalog.Catalog` instance, which is the
    typical way to resolve references from one resource to another.

    In some document formats, only certain nodes are relevant to resource
    identity.  This class supports the notion of a parent-in-resource,
    which is the nearest parent node that might impact resource identity.
    It also supports the determination of a resource root other than
    the document root.

    For example, in JSON Schema documents, only schema nodes can determine
    resource identity, and only the `"$id"` keyword indicates a resource
    root other than the document root.

    A resource can encompass multiple formats, such as in OpenAPI 3.1
    where a Schema Object is a JSON Schema-format structure, but is
    not a distinct resource from the OpenAPI document unless it
    contains `"$id"`.
    
    :class:`~jschon.json.JSON` subclasses representing document nodes
    in a URI-identified resource should inherit from this mixin and call
    :meth:`init_resource()` during construction.

    :class:`~jschon.json.JSON` subclasses that can contain references among
    or within documents must implement :meth:`resolve_references()`.
    """
    def __init__(
        self,
        value: JSONCompatible,
        *,
        parent: JSON = None,
        key: str = None,
        catalog: Union[str, Catalog] = 'catalog',
        cacheid: Hashable = 'default',
        uri: Optional[URI] = None,
        resolve_references: bool = True,
        itemclass: Type[JSON] = None,
        **itemkwargs: Any,
    ) -> None:
        if itemclass is None:
            itemclass = JSONResource
        super().__init__(
            value,
            parent=parent,
            key=key,
            itemclass=itemclass,
            **itemkwargs,
        )
        # self.init_resource(uri, catalog=catalog, cacheid=cacheid)
        if parent is None and resolve_references:
            self.resolve_references()

    def _pre_recursion_init(
        self,
        *args,
        uri: Optional[URI] = None,
        catalog: Union[Catalog, str] = 'catalog',
        cacheid: Hashable = 'default',
        **kwargs,
    ):
        """
        Configure identifiers and register with the catalog if needed.

        The resource is registered under the following conditions:

        * ``uri`` does not have a fragment
        * ``uri`` has an empty fragment, which is interpreted as a root
          JSON Pointer; the resource is then registered under the 
          semantically equivalent no-fragment URI
        * ``uri`` has a non-JSON Pointer fragment
        * ``uri`` is ``None`` and this node is a resource root,
          in which case a UUID URN is generated and registered

        The :class:`~jschon.catalog.Catalog` can resolve JSON Pointer fragment
        URIs without cluttering the cache with them.  The empty JSON Pointer
        fragment is special-cased due to its complex history in JSON Schema's
        ``"$id"`` keyword.

        :param uri: The URI under which this document is to be registered
            with the :class:`~jschon.catalog.Catalog`, which may be adjusted
            as explaine above.  If created by the catalog, this will be the
            URI used to request the resource.
        :param catalog: The catalog name or instance managing this resource.
        :param cacheid: The identifier for the cache within the catalog
            in which this resource is stored.
        """


        try:
            self.parent, self.path, self.document_root, self.resource_root
        except AttributeError:
            raise ResourceNotReadyError()

        if isinstance(catalog, str):
            from jschon.catalog import Catalog
            catalog = Catalog.get_catalog(catalog)

        self._uri: Optional[URI] = None
        self._catalog_uri: Optional[URI] = None
        self._base_uri: Optional[URI] = None

        self.catalog: Catalog = catalog
        self.cacheid: Hashable = cacheid
        self.references_resolved: bool = False
        
        # TODO: Support an initial base URI.  In practice this is
        #       not currently needed as relative "$id"s are handled
        #       after initialization.
        if uri is not None and not uri.has_absolute_base():
            raise RelativeResourceURIError()
        assert hasattr(self, '_uri')
        self.uri = uri

        frag = self._uri.fragment
        if frag in (None, '') or (frag[0] != '/'):
            # Add all non-JSON Pointer fragment URIs.  Strip the
            # empty JSON Pointer if relevant.
            if frag == '':
                self._uri = self._uri.copy(fragment=None)
            catalog.add_resource(self._uri, self, cacheid=cacheid)

    def _get_pointer_uri(self):
        root = self.resource_root
        ptr_from_root = self.path[len(root.path):]
        return root.uri.copy(fragment=ptr_from_root.uri_fragment())

    def uris_for(self, uri: Optional[URI]) -> ResourceURIs:
        """Determine the URIs for various use cases.

        Absolute URIs (without a fragment) and URIs with non-JSON Pointer
        fragments are registered as-is.

        JSON Pointer fragment URIs do not need to be registered with the
        catalog.  However, the empty JSON Pointer is semantically equivalent
        to not having a fragment, so in that case the absolute URI is returned.
        is returned.

        If ``uri`` is ``None`` and this document node is a resource root,
        a UUID URN is generated for catalog registration purposes.
        """
        # TODO: should UUID generation check for resource root-ness?
        # CATALOG, .uri, .ptr_uri
        if uri is None:
            if self.is_resource_root():
                urn = URI(f'urn:uuid:{uuid4()}')
                return ResourceURIs(
                    catalog_uri=urn,
                    property_uri=urn,
                    pointer_uri=urn.copy(fragment=''),
                    base_uri=urn,
                )

            ptr_uri = self._get_pointer_uri()
            return ResourceURIs(
                catalog_uri=None,
                property_uri=ptr_uri,
                pointer_uri=ptr_uri,
                base_uri=ptr_uri.copy(fragment=None),
            )

        fragment = uri.fragment
        if fragment is None:
            return ResourceURIs(
                catalog_uri=uri,
                property_uri=uri,
                pointer_uri=uri.copy(fragment=''),
                base_uri=uri,
            )

        if fragment == '':
            absolute_uri = uri.copy(fragment=None)
            return ResourceURIs(
                catalog_uri=absolute_uri,
                property_uri=absolute_uri,
                pointer_uri=uri,
                base_uri=absolute_uri,
            )

        if fragment[0] == '/':
            return ResourceURIs(
                catalog_uri=None,
                property_uri=uri,
                pointer_uri=uri,
                base_uri=uri.copy(fragment=None),
            )

        # Non-JSON Pointer fragment
        if self.is_resource_root():
            absolute = uri.copy(fragment=None)
            return ResourceURIs(
                catalog_uri=absolute,
                property_uri=uri,
                pointer_uri=uri.copy(fragment=''),
                base_uri=absolute,
            )

        ptr_uri = self._get_pointer_uri()
        return ResourceURIs(
            catalog_uri=uri,
            property_uri=uri,
            pointer_uri=ptr_uri,
            base_uri=ptr_uri.copy(fragment=None),
        )

    @property
    def parent_in_resource(self) -> JSONResource:
        """Returns the nearest parent node that is of a resource type, if any.

        The parent-in-resource may be of a different 
        """
        return self.parent
        candidate = None
        current = self

        while (candidate := current.parent) is not None:
            if isinstance(candidate, JSONResource):
                return candidate
            current = candidate
        return current

    @property
    def resource_root(self) -> JSONResource:
        candidate = self
        while (next_candidate := candidate.parent_in_resource) is not None:
            if next_candidate.is_resource_root():
                return next_candidate
            candidate = next_candidate

        # Without an explicit resource root, the document root is the
        # implicit resource root.
        return candidate.document_root

    def is_resource_root(self) -> bool:
        """True if no parent in the document is part of this same resource.

        Classes for documents that can contain multiple resources need to
        override this.
        """
        return self.parent_in_resource is None

    @property
    def uri(self) -> URI:
        """The URI identifying the resource in the catalog's cache.

        Note that if this URI has a JSON Pointer fragment, the catalog
        cache will only explicitly contain the :attr:`base_uri`.

        In come cases, a resource may be cached under multiple URIs.
        This URI is based on the one passed to :meth:`init_uri` or set
        through this attribute.
        """
        if self._uri is None:
            raise ResourceURINotSetError()
        return self._uri

    @uri.setter
    def uri(self, uri: Optional[URI]) -> None:
        """Set all URI properties from an input URI; see :meth:`uris_for`."""
        uris = self.uris_for(uri)

        if self._catalog_uri is None:
            self._catalog_uri = uris.catalog_uri
            self.catalog.add_resource(
                self._catalog_uri,
                self,
                cacheid=self.cacheid,
            )
        elif self._catalog_uri != uris.catalog_uri:
            self.catalog.del_resource(
                self._catalog_uri,
                cacheid=self.cacheid,
            )
            self._catalog_uri = uris.catlog_uri
            self.catalog.add_resource(
                self._catalog_uri,
                self,
                cacheid=self.cacheid,
            )

        self._uri = uris.property_uri
        self._pointer_uri = uris.pointer_uri
        self._base_uri = uris.base_uri

    @property
    def pointer_uri(self) -> URI:
        """The URI of this node using its base URI and a JSON Pointer fragment.

        This property is similar to JSON Schema's "canonical URI", but
        is unambiguous and consistent with respect to fragments.
        """
        if self._pointer_uri is None:
            raise ResourceURINotSetError()
        return self._pointer_uri

    @property
    def base_uri(self):
        """The absolute-URI against which relative references are resolved."""
        if self._base_uri is None:
            raise ResourceURINotSetError()
        return self._base_uri

    def resolve_references(self) -> None:
        """Resolve references, recursivey walking the document from this node.

        The exact behavior of reference resolution is determined by the
        :class:`~jschon.json.JSON` subclass implementing this interface.

        See also :attr:`references_resolved`; implementations should set
        :attr:`_references_resolved` to ``True`` to avoid expensive
        re-resolution.

        If references are not supported, reference resolution trivially
        succeeds.  This is the default behavior.
        """
        self.references_resolved = True
