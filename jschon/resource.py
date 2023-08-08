from __future__ import annotations

from uuid import uuid4

from typing import Any, ContextManager, Dict, Hashable, Iterator, Mapping, Optional, TYPE_CHECKING, Tuple, Type, Union

if TYPE_CHECKING:
    from jschon.catalog import Catalog, Source


class ResourceMixin:
    """Supports URIs and :class:`~jschon.catalog.Catalog` management.

    This class faciliates the extension of ``jschon`` to formats
    beyond JSON Schema, and is not intended for direct use.

    Resources have at least one URI and can serve as reference sources
    and / or targets.  They can be loaded by requesting them by URI
    from a :class:`~jschon.catalog.Catalog` instance, which is the
    typical way to resolve references from one resource to another.

    
    are resolved via the :class:`~jschon.catalog.Catalog` instance in
    which they are cached.  The resource is cached under its :attr:`uri`;
    see :meth:`init_resource` for how this is determined.

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
    :meth:`init_resource()` during construction.  Absolute-URIs (RFC 3986 §4.3)
    and URIs using fragment syntax other than JSON Pointer (RFC 6901)
    should be directly registered with the :class:`~jschon.catalog.Catalog`.

    :class:`~jschon.json.JSON` subclasses that can contain references among
    or within documents must implement :meth:`resolve_references()`.
    """
    def init_resource(
        self,
        uri: Optional[URI] = None,
        *,
        catalog: Union[Catalog, str] = 'catalog',
        cacheid: Hashable = 'default',
    ):
        """
        Configure identifiers and register with the catalog if needed.

        The resource is registered under the following conditions:

        * ``uri`` does not have a fragment
        * ``uri`` has an empty fragment, which is treated as a root
          JSON Pointer; the resource is then registered under the 
          semantically equivalent no-fragment URI
        * ``uri`` has a non-JSON Pointer fragment

        The :class:`~jschon.catalog.Catalog` can resolve JSON Pointer fragment
        URIs without cluttering the cache with them.

        Resources can 
        :param uri: The URI under which this document is to be registered
            with the :class:`~jschon.catalog.Catalog`.  If created by the
            catalog, this will be the URI used to request the resource.
        :param catalog: The catalog name or instance managing this resource.
        :param cacheid: The identifier for the cache within the catalog
            in which this resource is stored.
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

        self._catalog: Catalog = catalog
        self._cacheid: Hashable = cacheid
        self._references_resolved: bool = False

        # TODO: Optional
        self._uri: Optional[URI] = (
            URI(f'urn:uuid:{uuid4()}')
            if uri is None and self.parent is None
            else uri
        )

        if not self._uri.has_absolute_base():
            raise ValueError(f"Relative URI-reference not allowed!")

        frag = self._uri.fragment
        if frag in (None, '') or (frag[0] != '/'):
            # Add all non-JSON Pointer fragment URIs.  Strip the
            # empty JSON Pointer if relevant.
            if frag == '':
                self._uri = self._uri.copy(fragment=None)
            catalog.add_resource(self._uri, self, cacheid=cacheid)

    @property
    def parent_in_resource(self) -> ResourceMixin:
        """Returns the nearest parent node that is of a resource type, if any.

        The parent-in-resource may be of a different 
        """
        candidate = None
        while next_candidate := self.parent:
            if isinstance(next_candidate, ResourceMixin):
                candidate = next_candidate
        return candidate

    @property
    def resource_root(self) -> ResourceMixin:
        candidate = self
        while next_candidate := self.parent_in_resource:
            if next_candidate is not None:
                candidate = next_candidate
        return candidate

    def is_resource_root(self) -> bool:
        """True if no parent in the document is part of this same resource.

        Classes for documents that can contain multiple resources need to override this.
        """
        return self.parent_in_resource is None

    @property
    def uri(self) -> URI:
        """The URI identifying the resource in the catalog's cache.

        If instantiated through a :class:`Catalog`, this should be the URI by
        which it was requested.  If no URI was provided, the URI declared
        in the document (e.g. by ``"$id"``) should be used.  If no such URI
        exists, then a UUID URN is automatically generated by this mixin.
        """
        return self._uri if self._uri else self.pointer_uri

    @property
    def pointer_uri(self):
        """The URI of this node using its base URI and a JSON Pointer fragment.

        This property is similar to JSON Schema's "canonical URI", but
        is unambiguous and consistent with respect to fragments.
        """
        if self._uri:
            fragment = self._uri.fragment
            if fragment == '' or fragment[0] == '/':
                return self._uri
            if fragment is None:
                return self._uri.copy(fragment='')
        
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
