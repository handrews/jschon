from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from uuid import uuid4
import re
import urllib.parse

from typing import Any, ClassVar, Dict, FrozenSet, Generator, Hashable, Mapping, Optional, Set, TYPE_CHECKING, Type, Union

from jschon.exc import JschonError
from jschon.json import JSON
from jschon.jsonpointer import RelativeJSONPointer
from jschon.uri import URI

if TYPE_CHECKING:
    from jschon.catalog import Catalog, Source

__all__ = [
    'JSONResource',
    'JSONSchemaRefId',
    'RefIdKeywordConfig',
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
        if pre_recursion_args is None:
            pre_recursion_args = {}

        local_pre_recursion_args = {
            'catalog': catalog,
            'cacheid': cacheid,
            'uri': uri,
            'initial_base_uri': initial_base_uri,
            'default_uri_factory': default_uri_factory,
            'additional_uris': additional_uris,
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
            **itemkwargs,
        )

        if parent is None and resolve_references:
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

        self.references_resolved = False

        self._uri: Optional[URI] = None
        self._base_uri: Optional[URI] = None
        self._additional_uris: FrozenSet[URI] = frozenset()

        if isinstance(catalog, str):
            from jschon.catalog import Catalog
            catalog = Catalog.get_catalog(catalog)

        self.catalog: Catalog = catalog
        self.cacheid: Hashable = cacheid
        self.references_resolved: bool = False

        self._set_uri(uri, initial_base_uri, default_uri_factory)
        self.additional_uris |= {
            au.resolve(self._base_uri) for au in additional_uris
        }

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
        self.references_resolved = True


@dataclass
class RefIdKeywordConfig:
    """Convenience structure for configuring :class:`JSONSchemaRefId`"""

    reference_keywords: Set[str] = frozenset({"$ref", "$dynamicRef"})
    """Keywords that take a URI-reference pointing to another resource."""

    anchor_keywords: Set[str] = frozenset({"$anchor", "$dynamicAnchor"})
    """Keywords that define a plain-name fragment."""

    id_fragment_support: Literal['none', 'empty', 'plain-name', 'any'] = 'empty'
    """What fragmnent syntax/semantics, if any, are supported in "$id"."""

    allow_iris: bool = False
    """If true, allow IRIs (URIs with full unicode support per RFC 3987)."""


class JSONSchemaRefId(JSONResource):
    """Minimal static implementation of JSON Schema reference and id keywords.

    This is an experimental class loosely inspired by the JRI (JSON Referencing
    and identification) proposal at:
    https://github.com/json-schema-org/referencing (rendered a
     https://handrews.github.io/renderings/draft-handrews-jri.html).
    However, it implements only the keywords and behaviors present in
    JSON Schema, and provides options to support syntax back to draft-06.

    This class also supports the *static* aspects of dynamic keywords such as
    ``"$dynamicAnchor`` and ``"$dynamicRef"``, meaning aspects that do not
    require the concept of evaluating an instance.  The behavior of
    ``"$synamicAnchor"`` is already static, and the first step of resolving
    ``"$dynanicRef"`` is identical to that of ``"$ref"``.  Subclasses may
    build on this to offer dynamic support for these and potentially other
    keywords.

    Reference resolution is performed as far as connecting reference
    keywords and targets (initial targets in the case of ``"$dynamicRef"``).
    See :attr:``resolved_references`` for details.

    This class is intended to serve as a base class for working with formats
    such as OpenAPI and AsyncAPI that use a mix of JSON Schema / JSON Reference
    and other referencing / linking strategies, often in purely static contexts.
    It is distinct from the approach used by
    :class:`~jschon.jsonschema.JSONSchema`, which (correctly) integrates the
    handling of these keywords with its evaluation model.
    """
    FRAGMENT_SAFE_CHARACTERS="!$&'()*+,;=@:/?"

    URI_ANCHOR_REGEXP: ClassVar[re.Pattern] = re.compile(
        r'^[A-Za-z_][-A-Za-z0-9._]*$',
    )
    """US-ASCII XML NCName production US-ASCII subset (core metaschema)."""

    IRI_ANCHOR_REGEXP: ClassVar[re.Pattern] = re.compile(
        r'['
            r'_a-zA-Z' r'\xc0-\xd6' r'\xd8-\xf6' r'\xf8-\u02ff'
            r'\u0370-\u037d' r'\u037f-\u1fff' r'\u200c-\u200d' r'\u2070-\u218f'
            r'\u2c00-\u2fef' r'\u3001-\ud7ff' r'\uf900-\ufdcf' r'\ufdf0-\ufffd'
            r'\U00010000-\U000effff'
        r']'
        r'['
            r'_a-zA-Z' r'\xc0-\xd6' r'\xd8-\xf6' r'\xf8-\u02ff'
            r'\u0370-\u037d' r'\u037f-\u1fff' r'\u200c-\u200d' r'\u2070-\u218f'
            r'\u2c00-\u2fef' r'\u3001-\ud7ff' r'\uf900-\ufdcf' r'\ufdf0-\ufffd'
            r'\U00010000-\U000effff'
            r'\-.0-9\xb7' r'\u0300-\u036f' r'\u203f-\u2040'
        r']*'
    )
    """XML NCName production, genrated by abnf-to-regexp Python package."""

    def __init__(
        self,
        *args,
        ref_id_keyword_config: Optional[RefIdKeywordConfig] = None,
        **kwargs,
    ):
        self._ref_id_config = ref_id_keyword_config

        self.keyword_identifiers: Dict[str, URI] = {}
        """Identifier URIs organized by keyword."""

        self.keyword_references: Dict[str, URI] = {}
        """Reference target URIs organized by keyword."""

        super().__init__(*args, **kwargs)

    def _check_keywords(self, uri: Optional[URI], base_uri: URI):
        new_additional = set()
        if '$id' in self.data:
            id_uri = self._check_id(uri, base_uri)
            base_uri = id_uri.copy(fragment=None)
            self.keyword_identifiers['$id'] = id_uri
            if uri is not None:
                new_additional.add(uri)
            uri= id_uri

        for ak in self._ref_id_config.anchor_keywords:
            if ak in self.data:
                a_uri = base_uri.copy(
                    fragment=urllib.parse.quote(
                        self.data[ak],
                        safe=self.FRAGMENT_SAFE_CHARACTERS,
                    ),
                )
                self.keyword_identifiers[ak] = a_uri
                if uri is None:
                    uri = a_uri
                else:
                    new_additional.add(a_uri)

        for rk in self._ref_id_config.reference_keywords:
            if rk in self.data:
                r_uri = URI(self.data[rk]).resolve(base_uri)
                self.keyword_references[rk] = r_uri

        return uri, new_additional

    def _check_id(
        self,
        uri: Optional[URI],
        base_uri: URI,
    ) -> Tuple[URI, Set[URI]]:
        id_uri = URI(self.data['$id'])
        fs = self._ref_id_config.id_fragment_support
        if fs == 'none' and id_uri.fragment is not None:
            raise ValueError(f'"$id" <{id_uri}> must not have a fragment!')
        elif fs == 'empty' and id_uri.fragment not in (None, ''):
            raise ValueError(
                f'"$id" <{id_uri}> must not have a non-empty fragment!',
            )
        elif fs == 'plain-name':
            a = urllib.parse.unquote(id_uri.fragment)
            if self._ref_id_config.allow_iris:
                if self.IRI_ANCHOR_REGEXP.fullmatch(a) is None:
                    raise ValueError(
                        f'"$id" <{id_uri}> plain name fragment must match '
                        f'regular expression /{self.IRI_ANCHOR_REGEXP}/',
                    )
            elif self.URI_ANCHOR_REGEXP.fullmatch(a) is None:
                raise ValueError(
                    f'"$id" <{id_uri}> plain name fragment must be ascii and '
                    f'match regular expression /{self.URI_ANCHOR_REGEXP}/',
                )
        if (
            not id_uri.has_absolute_base()
        ):
            id_uri = id_uri.resolve(base_uri)

        return id_uri

    def pre_recursion_init(
        self,
        *,
        catalog: Union[Catalog, str] = 'catalog',
        cacheid: Hashable = 'default',
        uri: Optional[URI] = None,
        additional_uris: Set[URI] = frozenset(),
        initial_base_uri: Optional[URI] = None,
        default_uri_factory: Callable[[], URI] = DEFAULT_URI_FACTORY,
        **kwargs: Any,
    ):
        """Processes keywords to determine URIs prior to superclass invocation.

        All parameters not documented here behave identically to those for
        :meth:`jschon.resource.JSONResource.pre_recursion_init`.

        All paremeters controlling which keywords are supported and with
        what syntax default to the behavior of JSON Schema draft 2020-12.
        This class assumes that any keyword may appear in any object;
        subclasses may implement further restrictions.

        The base URI is determined in accordance with the JSON Schema
        specification and RFC 3986 §5.1 (subsections given for each point):

            1. From ``"$id"`` (§5.1.1)
            2. From a parent ``"$id"` (§5.1.2)
            3. From the the ``initial_base_uri`` parameter (§5.1.2 – 5.1.4)
            4. From the ``uri`` parameter as the request URI (§5.1.3)
            5. The ``default_uri_factory`` parameter (§5.1.4)

        The ``initial_base_uri`` parameter is assumed to represent the caller's
        interpretation of RFC 3986 §5.1.2 – 5.1.4, including base URI
        determination specific to the calling application, and therefore takes
        precedence over the assumptions involved in using the `uri` or
        `default_uri_factory` parameters.

        :param reference_keywords: The keywords to resolve as references,
            following the same rules as JSON Schema's ``"$ref"``.
        :param anchor_keywords: The keywords that define plain-name fragments
            as strings, relative to the current base URI.
        :param id_fragment_support: Whether the ``"$id"`` keyword allows
            fragments, and in what way.  The options are ``"none"`` (the
            proposed post-draft 2020-12 behavior, and the recommended usage
            in 2019-09 and 2020-12), ``"empty"`` (allowing empty JSON Pointer
            fragments as in 2019-09 and 2020-12), ``"plain-name"`` allowing
            the plain-name-fragment-defining behavior from draft-06 and -07
            that was later split out into ``"$anchor"``, or ``"any"``, allowing
            arbitrary fragment syntax.  Note that with ``"any"``, this class
            does not attempt to prevent nonsensical JSON Pointer fragment
            conflicts, and will raise a `NotImplementedError` if it does not
            recognize the fragment syntax.
        """
        if self._ref_id_config is None:
            self._ref_id_config = (
                self.resource_parent._ref_id_config if self.resource_parent
                else RefIdKeywordConfig()
            )

        if initial_base_uri is None:
            if uri is not None and uri.has_absolute_base:
                initial_base_uri = uri
            else:
                initial_base_uri = default_uri_factory()

        elif not initial_base_uri.is_absolute():
            raise ValueError(
                "Initial base URI must be an absolute-URI as defined "
                "by RFC 3986 §4.3",
            )

        base_uri = (
            self.resource_parent.base_uri if self.document_root != self
            else initial_base_uri
        )

        new_additional = set()
        if isinstance(self.data, Mapping):
            uri, new_additional = self._check_keywords(uri, base_uri)

        print(f'{self}: <{uri}> <<{additional_uris | new_additional}>>')
        super().pre_recursion_init(
            catalog=catalog,
            cacheid=cacheid,
            uri=uri,
            additional_uris=additional_uris | new_additional,
            initial_base_uri=base_uri,
            default_uri_factory=default_uri_factory,
            **kwargs,
        )

    def __setitem__(self, index, obj):
        super().__setitem__(index, obj)
        if index == '$id':
            id_uri = URI(obj)
            if id_uri.scheme is None:
                if (p := self.resource_parent) is not None:
                    id_uri = id_uri.resolve(p.base_uri)
                else:
                    # our past base URI was the base for the whole
                    # resource, and close enough for testing purposes
                    # even if potentially not technically correct.
                    id_uri = id_uri.resolve(self.base_uri)
            self.uri = URI(obj)

    def is_resource_root(self):
        if isinstance(self.data, (JSON, Mapping)):
            return (
                "$id" in self.data and not str(self.data['$id']).startswith('#')
                or self.parent is None
            )
        return self.parent is None
