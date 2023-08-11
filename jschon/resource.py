from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from uuid import uuid4

from typing import Any, ContextManager, Dict, FrozenSet, Generator, Hashable, Iterator, Mapping, Optional, Set, TYPE_CHECKING, Tuple, Type, Union

from jschon.exc import JschonError
from jschon.json import JSON
from jschon.uri import URI

if TYPE_CHECKING:
    from jschon.catalog import Catalog, Source

__all__ = [
    'JSONResource',
    'ResourceURIs',
    'ResourceError',
    'BaseURIConflictError',
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


class BaseURIConflictError(ResourceError):
    """Raised when attempting to set a URI in a way that conflicts with the established base URI for the resource."""


@dataclass(frozen=True)
class ResourceURIs:
    """Data structure for organizing resource document node URIs by use case."""

    register_uri: bool
    """Indicates if :attr:`property_uri` should be registered with the catalog."""

    property_uri: URI
    """The URI to return from the :attr:`uri` property."""

    base_uri: URI
    """The base URI for the resource."""

    additional_uris: Set[URI]
    """Alternate URIs to register with the catalog."""

    @classmethod
    def pointer_uri_for(cls, node):
        res_root = node.resource_root
        ptr_from_res_root = node.path[len(res_root.path):]
        return res_root.uri.copy(fragment=ptr_from_res_root.uri_fragment())

    @classmethod
    def uris_for(cls, node, uri: Optional[URI]) -> ResourceURIs:
        """Determine the URIs for various use cases.

        Absolute URIs (without a fragment) and URIs with non-JSON Pointer
        fragments are registered as-is.  If a non-JSON Pointer fragment
        is given for a resource root, the absolute form is assigned
        to :attr:`property_uri` (and :attr:`base_uri`) while the fragment URI
        is assigned to :attr:`additional_uris`.

        JSON Pointer fragment URIs do not need to be registered with the
        catalog.  However, the empty JSON Pointer is semantically equivalent
        to not having a fragment, so in that case the absolute portion of
        that URI is assigned to :attr:`property_uri` and :attr:`base_uri`.

        JSON Pointer fragment URIs never appear in :attr:`additional_uris`.

        If ``uri`` is ``None`` and this document node is a resource root,
        a UUID URN is generated for catalog registration purposes.
        """
        # TODO: should UUID generation check for resource root-ness?
        # CATALOG, .uri, .ptr_uri
        if uri is None:
            if node.is_resource_root():
                urn = URI(f'urn:uuid:{uuid4()}')
                return ResourceURIs(
                    register_uri=True,
                    property_uri=urn,
                    base_uri=urn,
                    additional_uris=set(),
                )

            ptr_uri = cls.pointer_uri_for(node)
            return ResourceURIs(
                register_uri=False,
                property_uri=ptr_uri,
                base_uri=node.resource_root.base_uri,
                additional_uris=set(),
            )

        fragment = uri.fragment
        if fragment is None:
            return ResourceURIs(
                register_uri=True,
                property_uri=uri,
                base_uri=uri,
                additional_uris=set(),
            )

        if fragment == '':
            absolute_uri = uri.copy(fragment=None)
            return ResourceURIs(
                register_uri=True,
                property_uri=absolute_uri,
                base_uri=absolute_uri,
                additional_uris=set(),
            )

        if fragment[0] == '/':
            return ResourceURIs(
                register_uri=False,
                property_uri=uri,
                base_uri=uri.copy(fragment=None),
                additional_uris=set(),
            )

        # Non-JSON Pointer fragment at root node
        if node.is_resource_root():
            absolute = uri.copy(fragment=None)
            return ResourceURIs(
                register_uri=True,
                property_uri=absolute,
                base_uri=absolute,
                additional_uris={uri},
            )

        # Non-JSON Pointer fragment at sub-document node
        ptr_uri = cls.pointer_uri_for(node)
        return ResourceURIs(
            register_uri=True,
            property_uri=uri,
            base_uri=ptr_uri.copy(fragment=None),
            additional_uris=set(),
        )


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

    Parameters not documented here are passed directly to the
    :class:`~jschon.json.JSON` constructor.

    :param catalog: The catalog name or instance managing this resource.
    :param cacheid: The identifier for the cache within the catalog
        in which this resource is stored.
    :param uri: The URI initially used to identify the resource.  If being
        instantiated directly, this is the URI (if any) assigned by the
        caller, and will be used ot determine the
        :class:`~jschon.catalog.Catalog` registration URI(s).  If being
        instantiated by the :class:`~jschon.catalog.Catalog`, this is
        the URI provided to the :meth:`~jschon.catalog.Catalog.get_resource`
        call (or any type-specific version of it, e.g. ``get_schema()``).
        See :meth:`ResourceURIs.uris_for` for how various cases, including
        no URI being provided, are handled.
    :param additional_uris: Additional URIs under which this resource
        should be registered.  The caller is responsible for ensuring
        that these URIs, along with the ``uri`` parameter, do not
        conflict with each other in problematic ways.  URIs with non-empty
        JSON Pointer fragments are ignored; see :meth:`ResourceURIs.uris_for`
        for an explanation.
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
        additional_uris: Set[URI] = frozenset(),
        resolve_references: bool = True,
        itemclass: Type[JSON] = None,
        **itemkwargs: Any,
    ) -> None:
        self.references_resolved = False

        self._uri: Optional[URI] = None
        self._base_uri: Optional[URI] = None
        self._additional_uris: FrozenSet[URI] = frozenset()

        # Intended for use by _pre_recursion_init() while avoiding
        # requiring the JSON class to know about URIs in order to
        # properly calculate child URIs from parent URIs.
        #
        # TODO: Should there be a more general "prep itemkwargs"
        #       hook for handling such things?
        self._tentative_uri: Optional[URI] = uri
        self._tentative_additional_uris: Set[URI] = additional_uris

        if itemclass is None:
            itemclass = JSONResource
        super().__init__(
            value,
            parent=parent,
            key=key,
            itemclass=itemclass,
            # The remaing args are received by JSON in **itemkwargs
            catalog=catalog,
            cacheid=cacheid,
            **itemkwargs,
        )

        # self.init_resource(uri, catalog=catalog, cacheid=cacheid)
        if parent is None and resolve_references:
            self.resolve_references()

    def _pre_recursion_init(
        self,
        *args,
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

        self.catalog: Catalog = catalog
        self.cacheid: Hashable = cacheid
        self.references_resolved: bool = False
        
        # TODO: Support an initial base URI.  In practice this is
        #       not currently needed as relative "$id"s are handled
        #       after initialization.
        # TODO: Done?
        uri = self._tentative_uri
        if uri is not None and not uri.has_absolute_base():
            raise RelativeResourceURIError()
        self.uri = uri
        self.additional_uris |= self._tentative_additional_uris

    @property
    def parent_in_resource(self) -> JSONResource:
        """Returns the nearest parent node that is of a resource type, if any.

        The parent-in-resource may be of a different 
        """
        candidate = None
        current = self

        while (candidate := current.parent) is not None:
            if isinstance(candidate, JSONResource):
                return candidate
            current = candidate
        return candidate


    @classmethod
    def _find_child_resource_nodes(cls, node) -> Generator[JSONResource]:
        """Returns in-resource nodes including roots of embedded resources.

        Implemented as a class method due to intervening non-resource nodes.
        """
        if node.type == 'object':
            child_iter = node.data.values()
        elif node.type == 'array':
            child_iter = iter(node.data)
        else:
            return

        for child in child_iter:
            if isinstance(child, JSONResource):
                yield child
            else:
                yield from cls._find_child_resource_nodes(child)

    @property
    def child_resource_nodes(self) -> Generator[JSONResource]:
        yield from self._find_child_resource_nodes(self)

    @property
    def child_resource_roots(self) -> Generator[JSONResource]:
        yield from (
            child for child in self._find_child_resource_nodes(self)
            if child.is_resource_root()
        )

    @property
    def children_in_resource(self) -> Generator[JSONResource]:
        """Chidren one in-resource level down, in the same resource."""
        yield from (
            child for child in self._find_child_resource_nodes(self)
            if not child.is_resource_root()
        )

    @property
    def resource_root(self) -> JSONResource:
        candidate = self
        while (next_candidate := candidate.parent_in_resource) is not None:
            assert isinstance(next_candidate, JSONResource), f"{next_candidate} | {next_candidate.path}"
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

    # TODO: This assumes that a node will know whether it is a resource root
    #       before setting a new absolute-URI (and therefore base URI).
    #       Is this correct, or should setting such a URI indicate that
    #       the node is a resource root?  We probably do not wnat to rely
    #       on callers always setting absolute-URIs correctly to determine
    #       resource-rootness.
    @property
    def uri(self) -> URI:
        """The URI identifying the resource in the catalog's cache.

        While :attr:`pointer_uri` gives the location of this node in
        the JSON structure of the resource, this attribute is intended
        to be externally-facing when relevant.  While the code does not
        enforce this, callers are recommended to prefer:

        * the :attr:`base_uri` if this is node is the resource root
        * a URI with a non-JSON Pointer fragment attached to :attr:`base_uri`
        * the :attr:`pointer_uri`

        If this node is the :attr:`resource_root`, the base URI can be
        changed by setting this property ao an absolute-URI, or to a
        URI with a non-JSON Pointer fragment (which will set :attr:`base_uri`
        to that URI's base).  Setting a non-resource-root URI that
        disagrees with :attr:`base_uri` is not allowed; see
        :attr:`additional_uris` for managing alternate URI registration.

        See :meth"`ResourceURIs.uris_for` for an explanation of how JSON
        Pointer fragment URIs are handled as catalog cache keyes.

        Assigning to this URI will unregister the old URI from the catalog
        and re-register this resource under the new URI, and update
        :attr:`base_uri` and :attr:`pointer_uri` if appropriate.
        """
        if self._uri is None:
            raise ResourceURINotSetError()
        return self._uri

    @uri.setter
    def uri(self, uri: Optional[URI]) -> None:
        uris = ResourceURIs.uris_for(self, uri)
        old_uri = self._uri
        old_base = self._base_uri

        if (
            not self.is_resource_root() and
            uris.base_uri != self.parent_in_resource.base_uri
        ):
            raise BaseURIConflictError()

        self._uri = uris.property_uri
        self._base_uri = uris.base_uri

        # TODO: Might we ever want to unregister existing additional URIs?
        self.additional_uris = self.additional_uris | uris.additional_uris

        if old_base is not None and old_base != self.base_uri:
            for child in self.children_in_resource:
                child.uri = self.base_uri.copy(fragment=child.uri.fragment)

        if uris.register_uri:
            if old_uri is not None and old_uri != uris.property_uri:
                self.catalog.del_resource(old_uri, cacheid=self.cacheid)
            self.catalog.add_resource(
                uris.property_uri,
                self,
                cacheid=self.cacheid,
            )

    @property
    def additional_uris(self) -> FrozenSet[URI]:
        """URIs other than :attr:`uri` registered with the catalog.

        This set is intended to be used for non-JSON Pointer-fragment
        URIs such as plain name fragments and alternate absolute-URIs
        under which this resource might be requested, none of which
        affect the resource's base URI or are automatically propagated
        to child resource nodes.

        URIs in this set with an empty JSON Pointer fragment are converted
        to the absolute-URI form, which is semantically equivalent.

        If :attr:`uri` (or its empty JSON Pointer fragment equivalent)
        is in the set assigned to this property, it will be ignored.

        To change :attr:`base_uri` and :attr:`pointer_uri`, assign to
        :attr:`uri` rather thian this attribute.
        
        This set is not guaranteed to be complete, as external code
        can add resources to catalogs directly.

        Setting this attribute will unregister removed URIs and register
        the added ones.  URIs with non-empty JSON Pointer fragments are
        dropped from this set for reasons described in the documentation
        for :meth:`uri-for`.  An empty JSON Pointer fragment will be stripped,
        and the result used (unless it is identical to :attr:`uri`, in which
        case it wil be ignored(.
        """
        return self._additional_uris

    @additional_uris.setter
    def additional_uris(self, uris: Set[URI]) -> None:
        uris = {
            u.copy(fragment=None) if u.fragment == '' else u
            for u in uris
            if u.fragment in (None, '') or u.fragment[0] != '/'
        }

        # Allow assigning before self.uri is set by accessing self._uri
        uris.discard(self._uri)

        for removed in self._additional_uris - uris:
            self.catalog.del_resource(removed, cacheid=self.cacheid)
        for added in uris - self._additional_uris:
            self.catalog.add_resource(added, self, cacheid=self.cacheid)

        self._additional_uris = frozenset(uris)

    def _invalidate_path(self) -> None:
        super()._invalidate_path()
        try:
            uri_is_pointer_uri = self.pointer_uri == self.uri
            del self.pointer_uri

            if uri_is_pointer_uri:
                self.uri = self.pointer_uri

        except (ResourceURINotSetError, AttributeError):
            pass

    @cached_property
    def pointer_uri(self) -> URI:
        """The URI of this node using its base URI and a JSON Pointer fragment.

        This property is similar to JSON Schema's "canonical URI", but
        is unambiguous and consistent with respect to fragments.
        """
        return ResourceURIs.pointer_uri_for(self)

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
