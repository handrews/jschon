from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from uuid import uuid4

from typing import Any, Dict, FrozenSet, Generator, Hashable, Optional, Set, TYPE_CHECKING, Type, Union

from jschon.exc import JschonError
from jschon.json import JSON
from jschon.uri import URI

if TYPE_CHECKING:
    from jschon.catalog import Catalog, Source

__all__ = [
    'JSONResource',
    'ResourceURIs',
    'ResourceError',
    'ResourceNotReadyError',
    'ResourceURINotSetError',
    'RelativeResourceURIError',
    'DuplicateResourceURIError',
    'InconsistentResourceRootError',
    'UnRootedResourceError',
]


DEFAULT_URI_FACTORY = lambda: URI(f'urn:uuid:{uuid4()}')
"""The default generator for default URIs when none is specified."""


class ResourceError(JschonError):
    """An error originating in the :mod:`~jschon.resource` module."""


class ResourceNotReadyError(ResourceError):
    """Raised if the ``path``, ``parent``, and ``resource_root`` attributes are not availalbe on resource initialization."""


class ResourceURINotSetError(ResourceError):
    """Raised when accessing any URI property before the URI has been set."""


class RelativeResourceURIError(ResourceError):
    """Raised when attempting to set a URI to a relative URI-reference without an available base URI."""


class DuplicateResourceURIError(ResourceError):
    """Raised if an empty relative URI is used for a resource root, which produces a duplicate URI of the resource that provided the initial base URI."""


class InconsistentResourceRootError(ResourceError):
    """Raised when :attr:`resource_root` and :meth:`is_resource_root` disagree during instantiation."""


class UnRootedResourceError(ResourceError):
    """Raised when there is no :class:`JSONResource` root in the resource.

    This can happen if neither the current node nor any of its ancestors
    are explicitly marked as resoruce roots in some way, and the document
    root node, which is the default resource root, is not
    a :class:`JSONResource` instance.
    """


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
    def uris_for(
        cls,
        node,
        uri: Optional[URI],
        initial_base_uri: Optional[URI],
        default_uri_factory: Callable[[], URI],
    ) -> ResourceURIs:
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

        :param node: The :class:`JSONResource` node for which the URIs are
            being deterimined
        :param uri: See :meth:`pre_recursion_init`
        :param initia_base_uri: See :meth:`pre_recursion_init`
        :param default_uri_factory: See :meth:`pre_recursion_init`
        """
        if uri is None:
            if node.is_resource_root():
                default = default_uri_factory()
                return ResourceURIs(
                    register_uri=True,
                    property_uri=default,
                    base_uri=default,
                    additional_uris=set(),
                )

            ptr_uri = cls.pointer_uri_for(node)
            return ResourceURIs(
                register_uri=False,
                property_uri=ptr_uri,
                base_uri=node.resource_root.base_uri,
                additional_uris=set(),
            )

        if not uri.has_absolute_base():
            if str(uri) == '':
                raise DuplicateResourceURIError()
            if node.is_resource_root():
                if initial_base_uri is None:
                    initial_base_uri = default_uri_factory()
                elif not initial_base_uri.has_absolute_base():
                    initial_base_uri = initial_base_uri.resolve(
                        default_uri_factory()
                    )
                base_uri = initial_base_uri
            else:
                base_uri = node.resource_root.base_uri
            uri = uri.resolve(base_uri)

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
    :class:`~jschon.json.JSON` subclasses representing document nodes
    in a URI-identified resource should inherit from this class, which
    overrides :meth:`jschon.json.JSON.pre_recursion_init` to set up
    the resource infrastructure.
.
    :class:`~jschon.json.JSON` subclasses that can contain references among
    or within documents must implement :meth:`resolve_references()`.

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

    All constructor parameters are either passed through directly to
    :meth:`jschon.json.JSON.__init__` or are passed via its
    ``pre_recursion_args`` parameter to :meth:pre_recursion_init`;
    see those methods for parameter documentation.  The one exception
    is ``pre_recursion_args`` itself, which is merged with but in cases
    of conflict takes precedence over the pre-recursion arguments
    produced by this class.
    """

    @classmethod
    def _find_child_resource_nodes(cls, node) -> Generator[JSONResource]:
        """Returns in-resource nodes including roots of embedded resources.

        Implemented as a class method to allow recursing through intervening
        non-:class:`JSONResource` nodes.

        When the starting node is a :class:`JSONResource` instance, callers
        should use :attr:`child_resource_nodes:`.
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

    def __init__(
        self,
        value: JSONCompatible,
        *,
        parent: JSON = None,
        key: str = None,
        catalog: Union[str, Catalog] = 'catalog',
        cacheid: Hashable = 'default',
        uri: Optional[URI] = None,
        initial_base_uri: Optional[URI] = None,
        default_uri_factory: Callable[[], URI] = DEFAULT_URI_FACTORY,
        additional_uris: Set[URI] = frozenset(),
        resolve_references: bool = True,
        pre_recursion_args: Optional[Dict[str, Any]] = None,
        itemclass: Type[JSON] = None,
        **itemkwargs: Any,
    ) -> None:

        # Set here and in pre_recursion_init() to handle both normal
        # subclasses those that don't invoke their superclass constructor.
        self._auto_resolve_references: bool = resolve_references

        if pre_recursion_args is None:
            pre_recursion_args = {}

        local_pre_recursion_args = {
            'catalog': catalog,
            'cacheid': cacheid,
            'uri': uri,
            'initial_base_uri': initial_base_uri,
            'default_uri_factory': default_uri_factory,
            'additional_uris': additional_uris,
            'resolve_references': resolve_references,
        }
        local_pre_recursion_args.update(pre_recursion_args)

        if itemclass is None:
            itemclass = type(self)
        super().__init__(
            value,
            parent=parent,
            key=key,
            itemclass=itemclass,
            pre_recursion_args=local_pre_recursion_args,
            resolve_references=resolve_references,
            **itemkwargs,
        )

        if parent is None and self._auto_resolve_references:
            self.resolve_references()

    def pre_recursion_init(
        self,
        *,
        catalog: Union[Catalog, str] = 'catalog',
        cacheid: Hashable = 'default',
        uri: Optional[URI] = None,
        additional_uris: Set[URI] = frozenset(),
        initial_base_uri: Optional[URI] = None,
        default_uri_factory: Callable[[], URI] = DEFAULT_URI_FACTORY,
        resolve_references: bool = True,
        **kwargs: Any,
    ):
        """
        Configure identifiers and register with the catalog if needed.

        This method is intended to be called by ``__init__()`` methods only.

        The resource is registered under the following conditions:

        * ``uri`` does not have a fragment
        * ``uri`` has an empty fragment, which is interpreted as a root
          JSON Pointer; the resource is then registered under the
          semantically equivalent absolute-URI (as defiend by RFC 3986 §4.3)
        * ``uri`` has a non-JSON Pointer fragment
            * If also a resource root, the absolute-URI form is also registered
        * ``uri`` is ``None`` and this node is a resource root, in which case
        a UUID URN is generated and registered

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
        :param uri: The primary URI initially used to identify the resource.
            This is handled and, if appropriate, registered with the relevant
            :class:`~jschon.catalog.Catalog`, as documented above.  If it
            differs from a relative URI specified in the content, and if no
            ``initial_base_uri`` parameter is provided, it is treated as
            the retrieval URI in accordance with RFC 3986 §5.1.3, as that is
            how the :class:`~jschon.catalog.Catalog` sets it when instantiating
            a resource retrieved from storage.
        :param additional_uris: Additional URIs under which this resource
            should be registered.  The caller is responsible for ensuring
            that these URIs, along with the ``uri`` parameter, do not
            conflict with each other in problematic ways.  URIs with non-empty
            JSON Pointer fragments are ignored;
            see :meth:`ResourceURIs.uris_for` for an explanation.
        :param initial_base_uri: A base URI, assumed to be determined in
            accordance with RFC 3986 §5.1, against which a relative ``uri``
            parameter should be resolved.  If no initial base URI is passed
            and one is needed, one will be generated using the
            ``default_uri_factory`` parameter.
        :param default_uri_factory: A callable that produces a unique URI
            on each invocation so that anonymous resources can be assigned
            a URI for internal :class:`~jschon.catalog.Catalog` registration.
        """
        try:
            # This ensures we have a valid resource root, which we should
            # know by now, and that the parent structure is initialized.
            if (
                (not self.resource_root.is_resource_root()) or
                (self.is_resource_root() and self.resource_root is not self)
            ):
                raise InconsistentResourceRootError()

        except AttributeError:
            raise ResourceNotReadyError()

        self.references_resolved: bool = False
        """``True`` if all references have been resolved by walking all (sub)schemas."""

        self._auto_resolve_references: bool = resolve_references

        self._uri: Optional[URI] = None
        self._base_uri: Optional[URI] = None
        self._additional_uris: FrozenSet[URI] = frozenset()

        if isinstance(catalog, str):
            catalog = self._get_catalog(catalog)

        self.catalog: Catalog = catalog
        self.cacheid: Hashable = cacheid
        self.references_resolved: bool = False

        self._set_uri(uri, initial_base_uri, default_uri_factory)
        self.additional_uris |= {
            au.resolve(self._base_uri) for au in additional_uris
        }

    def _get_catalog(self, catalog_str: str) -> Catalog:
        """Get a catalog by name from the correct Catalog subclass."""

        from jschon.catalog import Catalog
        return Catalog.get_catalog(catalog_str)

    def _set_uri(
        self,
        uri: Optiona[URI],
        initial_base_uri: Optional[URI] = None,
        default_uri_factory: Callable[[], URI] = DEFAULT_URI_FACTORY,
    ):
        """Implements URI-setting with additional options for initialization.

        External callers should assign to :attr:`uri`.
        """
        uris = ResourceURIs.uris_for(
            self,
            uri,
            initial_base_uri,
            default_uri_factory,
        )
        old_uri = self._uri
        old_base = self._base_uri

        self._uri = uris.property_uri
        self._base_uri = uris.base_uri

        self.additional_uris = self.additional_uris | uris.additional_uris

        if old_base is not None and old_base != self.base_uri:
            try:
                del self.pointer_uri
            except AttributeError:
                pass
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

    def _invalidate_value(self) -> None:
        """Causes value-dependent cached attributes to be re-calculated."""
        for attr in (
            'child_resource_nodes',
            'child_resource_roots',
            'children_in_resource',
        ):
            try:
                delattr(self, attr)
            except AttributeError:
                pass
        super()._invalidate_value()

    def _invalidate_path(self) -> None:
        """Causes path-dependent cached attributes to be re-calculated."""
        try:
            uri_is_pointer_uri = self.pointer_uri == self.uri
        except (ResourceURINotSetError):
            uri_is_pointer_uri = False

        for attr in (
            'pointer_uri',
            'resource_parent',
            'parent_in_resource',
            'resource_root',
        ):
            try:
                delattr(self, attr)
            except (ResourceURINotSetError, AttributeError):
                pass

        super()._invalidate_path()
        if uri_is_pointer_uri:
            self.uri = self.pointer_uri

    def is_resource_root(self) -> bool:
        """True if this node is at the root of a distinct resource.

        This base class defines resource_root-ness to be equivalent
        to :attr:`document_root`.  Resources that can have non-document-root
        resource roots must subclass and override this method.

        The document root should always be considered a resource root.
        """
        return self.parent is None

    @cached_property
    def resource_root(self) -> JSONResource:
        """The root this resource, which can differ from :attr:`document_root`.

        See also :meth:`is_resource_root`; if no other ancestor node is
        indicated as a resource root, this should be equivalent to
        :attr:`document_root`.
        """
        candidate = self
        while candidate is not None:
            if candidate.is_resource_root():
                return candidate
            candidate = candidate.parent_in_resource

        raise UnRootedResourceError()

    @cached_property
    def resource_parent(self) -> Optional[JSONResource]:
        """Returns the nearest ancestor that is a :class:`JSONResource`.

        This ancestor may or may not be part of the same resource, see
        also :attr:`parent_in_resource`"""
        candidate = None
        current = self

        while (candidate := current.parent) is not None:
            if isinstance(candidate, JSONResource):
                return candidate
            current = candidate
        return candidate

    @cached_property
    def parent_in_resource(self) -> Optional[JSONResource]:
        """Returns the nearest ancestor resource node in the same resource.

        This skips any intervening non-:class:`JSONResource` ancestor nodes,
        and returns ``None`` if this node is a resource root.
        """
        if self.is_resource_root():
            return None

        return self.resource_parent

    @cached_property
    def child_resource_nodes(self) -> Generator[JSONResource]:
        """All immediate :class:`JSONResource` descendents.

        This ignores intervening non-:class:`JSONResource` descendants,
        but does not distinguish between descendents in the same resource
        or in a distinct embedded resource.
        """
        yield from self._find_child_resource_nodes(self)

    @cached_property
    def child_resource_roots(self) -> Generator[JSONResource]:
        """Immediate descendents that are roots of embedded resources.

        :attr:`child_resource_nodes` with non-roots filtered out.
        """
        yield from (
            child for child in self._find_child_resource_nodes(self)
            if child.is_resource_root()
        )

    @cached_property
    def children_in_resource(self) -> Generator[JSONResource]:
        """Immediate :class:`JSONResource` descendents in the same resource.

        :attr:`child_resource_nodes`, but with roots of distinct embedded
        resources filtered out, leaving only children within the same resource.
        """
        yield from (
            child for child in self._find_child_resource_nodes(self)
            if not child.is_resource_root()
        )

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
        self._set_uri(uri)

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
        # Avoid locking in the child_resource_nodes cached property
        # as this method is often called relatively early in initialization.
        for child in self._find_child_resource_nodes(self):
            child.resolve_references()
        self.references_resolved = True
