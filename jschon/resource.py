from __future__ import annotations

from uuid import uuid4

from typing import Any, ContextManager, Dict, Hashable, Iterator, Mapping, Optional, TYPE_CHECKING, Tuple, Type, Union

if TYPE_CHECKING:
    from jschon.catalog import Catalog, Source


class ResourceMixin:
    """Supports URIs and :class:`~jschon.catalog.Catalog` management.

    Resources can serve as reference sources and / or targets, which
    are resolved via the :class:`~jschon.catalog.Catalog` instance in
    which they are cached.

    The resource is cached under its :attr:`uri`; see
    :meth:`init_resource` for how this is determined.
    
    :class:`~jschon.json.JSON` subclasses representing document nodes
    in a URI-identified resource should inherit from this mixin and call
    :meth:`init_resource()` during construction.  Absolute-URIs (RFC 3986 §4.3)
    and URIs using fragment syntax other than JSON Pointer (RFC 6901)
    are directly registered with the :class:`~jschon.catalog.Catalog`.

    See the documentation for :meth:`init_resource`, :attr:`uri`,
    :attr:`canonical_uri`, :attr:`base_uri`, and :attr:`url` for an
    explanation of each possible URI and when and how it should be used.

    :class:`~jschon.json.JSON` subclasses that can contain references among
    or within documents must implement :meth:`resolve_references()`.


    Resources can be identified by multiple URIs, which may each function as
    identifiers (URIs), locators (URLs), or both.

    A *canonical URI* (based on RFC 6596 "The Canonical Link Relation")
    is the URI indicated by the document or its retrieval meta-data (e.g.
    HTTP headers) as the preferred URI for accessing the resource.

    As this mixin is inteded for nodes within a document, its
    :
    While RFC 6589 only discusses URIs / IRIs
    for complete resources (without a URI fragment), this mixin is intended
    for use with classes implementing nodes in a JSON document.  
    absolute URIs (RFC 3986 §4.3

    As this mixin is intended for use by nodes within a document tree,
    the 

    A URI functions as a URL if it is intended to be used to interact with
    the resource by communicating with the URI's authority in accordance
    with the URI's scheme.  The :class:`~jschon.catalog.Catalog` implements
    a mapping between URIs and resources, which are retrieved through
    a :class:`~jschon.catalog.Source`.
    """
    def init_resource(
        self,
        catalog_uri: Optional[URI] = None,
        content_uri: Optional[URI] = None,
        initial_base_uri: Optional[URI] = None,
        source_url: Optional[URL] = None,
        source_metadata: Optional[Dict] = None,
        catalog: Union[Catalog, str] = 'catalog',
        cacheid: Hashable = 'default',
    ):
        """
        Configure identifiers and register with the catalog if needed.

        Resources can have several URIs:

        * a URI declare in content (JSON Schema's ``"$id"``)
        * a URI used as a cache identifier in the catalog
        * the URL from which the catalog actually loaded the contents

        The catalog functions as a proxy and cache for retrieving resources,
        so their cache key URI is considered the "request URI" for the
        purposes of RFC 3986 §5.1.3.

        The source URL is intended only for use in debugging and error
        reporting.  For example, the catalog key might represent a deployed
        location, 
        the catalog nor the one declared in 
        * The URI used to request the resource from the catalog, which
          should match the intended identity of the resource, such as
          its production location.  
          is the "retrieval URI" in RFC 3986 terms as the catalog
          functions as a cache abstracting away the actual st
        A the :class:`~jschon.catalog.Catalog` implements a cache that
        abstracts the 
        The resource is cached under its :attr:`uri`, which is the fully
        resolved URI determined in the following order:

        1. The ``catalog_uri`` parameter
        2. The ``content_uri`` parameter
        3. An auto-generated UUID URN

        If provided, the ``initial_base_uri`` parameter is assumed to be
        in compliance with RFC 3986 §5.1.2–5.1.4 for determinig a base
        URI outside of the resource contents.  This can represent the
        intended 
        in the c
        The ``source_url`` indicates how a resource was loaded into the
        catalog's cache, if relevant.  It is intended for debugging and
        error reporting purposes and is **not** considered the
        "retrieval URI" for the purpose of establishing a base URI.
        This is because the source location is considered an implementation
        detail of the catalog's cache.

        Since the :class:`~jschon.catalog.Catalog` functions as a cache,
        even if backed by the local filesystem or a remote HTTPS site,
        the ``source_url
        :param catalog_uri: The URI under which this document is to be
            registered with the :class:`~jschon.catalog.Catalog`.  If created
            by the catalog, this will be the URI used to request the resource.
            It is safe to re-register the resource with the catalog under
            the same URI.
        :param content_uri: The URI for this resource as defined
            in its contents.  If no ``catalog_uri`` is passed, this is used
            as the catalog cache key instead.
        :param initial_base_uri: The base URI used to calculate the final base
            URI if ``content_uri`` is relative; if no ``content_uri`` is
            provided it can be the final base URI itself; see :attr:`base_uri`
            for details.  Note that callers may opt to resolve a relative
            ``content_uri`` prior to calling this method rather than passing
            ``initial_base_uri``.
        :param source_url:  The URL from which the document containing this
            resource (which may be larger than the resource) was actually
            loaded, if any.  This should be `None` if the resource was
            instantiated from memory or if the calling code does not know
            its source URL.
        """
        try:
            parent = self.parent
            path = self.path
            docroot = self.document_root
        except AttributeError:
            from jschon.json import JSON
            if isinstance (self, JSON):
                raise ValueError(
                    "'self.parent', 'self.path', and 'self.document_root' "
                    "must be initialized before calling "
                    "'ResourceMixin.init_resource()'."
                )
            else:
                raise ValueError(
                    "ResourceMixin is intended to be used with "
                    "jschon.json.JSON subclasses, and requires 'self.parent' "
                    "'self.path', andn `self.document_root'  attributes. This "
                    f"object is an instance of {self.__class__.__name__!r}."
                )

        if isinstance(catalog, str):
            from jschon.catalog import Catalog
            catalog = Catalog.get_catalog(catalog)
        self._catalog = catalog
        self._cacheid = cacheid

        if initial_base_uri is not None and not initial_base_uri.is_absolute():
            raise ValueError(
                f"If provided, initial_base_uri <{initial_base_uri}> must be "
                "an absolute-URI (with a scheme, and without a fragment)"
            )

        if catalog_uri is not None and not catalog_uri.has_absolute_base():
            raise ValueError(
                f"If provided, catalog_uri <{catalog_uri}> must begin "
                "with a scheme!"
            )

        if content_uri is not None and content_uri.has_absolute_base():
                if initial_base_uri is not None:
                    content_uri = content_uri.resolve(initial_base_uri)
                elif catalog_uri is not None:
                    content_uri = content_uri.resolve(catalog_uri)
                else:
                    raise ValueError(
                        f"Cannot resolve relative URI reference
                        <{content_uri}> without a base URI!"
                    )
        register = False
        auto_uri = False
        if catalog_uri is None:
            catalog_uri = URI(f'urn:uuid:{uuid4()}') 
            auto_uri = True
            register = True
        elif not catalog_uri.has_absolute_base():
            raise ValueError(
                f"{self.__class__.__name__} cannot use relative URI "
                f"reference '{catalog_uri}` as a jschon.Catalog cache key!"
            )
        else:
            # Don't register JSON Pointer fragment URIs
            fragment = catalog_uri.fragment
            if fragment is None or (
                fragment != '' and not fragment.startswith('/')
            ):
                register = True

        catalog.add_schema(catalog_uri, self, cacheid=cacheid)

        self._references_resolved: bool = False
        self._uri: URI = if uri is None else uri

        if root == self:
            self._source_url: Optional[URI] = source_url
            self._source_metadata: Dict = (
                {} if source_metadata is None else source_metadata
            )
        elif (
            (source_url is not None and source_url != root._source_url) or
            (
                source_metadata is not None and
                source_metadata != root._source_metadata
            )
        ):
            raise ValueError(
                f"Source URL '{source_url}' or metadata conflicts with "
                f"root source URL '{root._source_uri}' or metadata!"
            )
        else:
            self._source_url = None
            self._source_metadata = {}

    @property
    def parent(self):
        raise NotImplementedError

    @property
    def uri(self):
        """The URI identifying the resource in the catalog's cache.

        If instantiated through a :class:`Catalog`, this should be the URI by
        which it was requested.  If no URI was provided, the URI declared
        in the document (e.g. by ``"$id"``) should be used.  If no such URI
        exists, then a UUID URN is automatically generated by this mixin.
        """
        return self._uri

    @property
    def canonical_uri(self):
        """The preferred URI for identifying this node in the resource.

        This concept is based on RFC 6596 "The Canonical Link Relation".
        While that RFC does not address URI fragments, since this mixin
        is used to implement nodes in a JSON document tree, this attribute
        always includes the JSON Pointer fragment for the node.

        If the resource declares a URI in its content, this URI will
        always have the same :attr:`base_uri` as that URI, plus the
        appropriate JSON Pointer fragment.  This may be different from
        :attr:`uri`.
        """
        return self._canonical_uri

    @property
    def base_uri(self):
        """The absolute-URI against which relative references are resolved.

        RFC 3986 §5.1 defines the process for determining a base URI.
        This mixin implements that process by checking URIs passed to
        :meth:`init_resource()` in the following order:

        1. ``content_uri`` (§5.1.1)
        2. ``initial_base_uri`` (assumed to represent §5.1.2–5.1.4)
        3. ``catalog_uri`` (retrieval URI per §5.1.3)

        If the URI at one step is a relative URI-reference, it is first
        resolved against the base URI from the next available step.

        catalog URIs cannot be relative, there is no need for further steps.
        Note that if no ``catalog_uri`` is explicitly passed, a UUID URN
        is generated to fill that role.
        """
        return self._base_uri

    @property
    def document_url(self) -> Optional[URI]:
        """The URL from which the resource's content was actually loaded.

        This is an absolute-URL for the entire document containing this
        resource, which may be a larger data structure with a different
        :attr:`base_uri` from this resource.
        """
        return self.document_root._source_url

    @property
    def document_metadata(self, field) -> Optional[Any]:
        return self.document_root._source_metadata.get(field)

    @property
    def references_resolved(self) -> bool:
        """Indicates whether references have been fully resolved.

        In most cases, references can be resolved during construction.
        However, see :doc:`/tutorial/catalog` for scenarios requiring
        :meth:`resolve_references` to be called after docuemnts with
        complex mutual references have all been instantiated.
        """
        return self._references_resolved

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
        self._references_resolved = True
