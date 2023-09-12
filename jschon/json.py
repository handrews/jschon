from __future__ import annotations

import json
from collections import deque
from functools import cached_property
from os import PathLike
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, MutableSequence, Optional, Sequence, Type, Union

from jschon.exceptions import JSONError, JSONPointerError
from jschon.jsonpointer import JSONPointer
from jschon.utils import json_dumpf, json_dumps, json_loadf, json_loadr, json_loads

__all__ = [
    'JSON',
    'JSONCompatible',
    'null',
    'true',
    'false',
]

JSONCompatible = Union[None, bool, int, float, str, Sequence[Any], Mapping[str, Any]]
"""Type hint for a JSON-compatible Python object."""

null = None
"""Use to represent the JSON `null` value literally in Python code."""

true = True
"""Use to represent the JSON `true` value literally in Python code."""

false = False
"""Use to represent the JSON `false` value literally in Python code."""


class JSON(MutableSequence['JSON'], MutableMapping[str, 'JSON']):
    """An implementation of the JSON data model."""

    @classmethod
    def loadf(cls, path: Union[str, PathLike], **kwargs: Any) -> JSON:
        """Deserialize a JSON file to a :class:`JSON` instance.

        :param path: the path to the file
        :param kwargs: keyword arguments to pass to the :class:`JSON` (subclass) constructor
        """
        return cls(json_loadf(path), **kwargs)

    @classmethod
    def loadr(cls, url: str, **kwargs: Any) -> JSON:
        """Deserialize a remote JSON resource to a :class:`JSON` instance.

        :param url: the URL of the resource
        :param kwargs: keyword arguments to pass to the :class:`JSON` (subclass) constructor
        """
        return cls(json_loadr(url), **kwargs)

    @classmethod
    def loads(cls, value: str, **kwargs: Any) -> JSON:
        """Deserialize a JSON string to a :class:`JSON` instance.

        :param value: the JSON string
        :param kwargs: keyword arguments to pass to the :class:`JSON` (subclass) constructor
        """
        return cls(json_loads(value), **kwargs)

    def __init__(
            self,
            value: JSONCompatible,
            *,
            parent: JSON = None,
            key: str = None,
            itemclass: Type[JSON] = None,
            pre_recursion_args: Optional[Dict[str, Any]] = None,
            **itemkwargs: Any,
    ):
        """Initialize a :class:`JSON` instance from the given JSON-compatible
        `value`.

        The `parent`, `key`, `pre_recursion_args`, `itemclass` and `itemkwargs`
        parameters should typically only be used in the construction of
        compound :class:`JSON` documents by :class:`JSON` subclasses.

        The use of the `parent`, `key`, `itemclass`, and `itemkwargs`
        parameters can be customized in subclasses by overriding
        :meth:`instantiate_sequence` and :meth:`instantiate_mapping`, for
        example if some child elemnts need to be instances of a different
        class than others.  Child elements instantiated in this way should
        still be instances of `itemclass` through inheritance, and
        `itemkwargs` should be respected if at all possible.

        :param value: a JSON-compatible Python object
        :param parent: the parent node of the instance
        :param key: the index of the instance within its parent
        :param pre_recursion_args: arguments to pass to
            :meth:`pre_recursion_init` during construction
        :param itemclass: the :class:`JSON` subclass used to instantiate
            child nodes of arrays and objects (default: :class:`JSON`)
        :param itemkwargs: keyword arguments to pass to the `itemclass`
            constructor
        """
        if pre_recursion_args == None:
            pre_recursion_args = {}

        self.type: str
        """The JSON type of the instance. One of
        ``null``, ``boolean``, ``number``, ``string``, ``array``, ``object``."""

        self.data: Union[None, bool, int, float, str, List[JSON], Dict[str, JSON]]
        """The instance data.

        =========   ===============
        JSON type   data type
        =========   ===============
        null        None
        boolean     bool
        number      int | float
        string      str
        array       list[JSON]
        object      dict[str, JSON]
        =========   ===============
        """

        self.parent: Optional[JSON] = parent
        """The containing JSON instance."""

        self.key: Optional[str] = key
        """The index of the instance within its parent."""

        self.itemclass: Type[JSON] = itemclass or JSON
        """The :class:`JSON` class type of child instances."""

        self.itemkwargs: Dict[str, Any] = itemkwargs
        """Keyword arguments to the :attr:`itemclass` constructor."""

        # TODO: Is this the right set of args?  Should there be another way to
        #       push these args through?
        for arg, default  in (('catalog', 'catalog'), ('cacheid', 'default')):
            self.itemkwargs.setdefault(
                arg,
                pre_recursion_args.get(arg, default),
            )

        # During recursive construction, the data attribute is by definition
        # incompletely initialized.  Setting it to the value up front is
        # correct for scalar types and allows setting the type attribute and
        # allows for limited use of the data attribute on parent nodes during
        # child node construction.  While not an ideal state, it is closer
        # to the proper state for data than not having the attribute at all.
        #
        # Proper recursive initialiazation is stored as a callable and carried
        # out after the pre-recursion hook is executed.
        self.data = value
        instantiate_sequence = False
        instantiate_mapping = False

        if value is None:
            self.type = "null"

        elif isinstance(value, bool):
            self.type = "boolean"

        elif isinstance(value, (int, float)):
            self.type = "number"

        elif isinstance(value, str):
            self.type = "string"

        elif isinstance(value, Sequence):
            self.type = "array"
            instantiate_sequence = True

        elif isinstance(value, Mapping):
            self.type = "object"
            instantiate_mapping = True

        else:
            raise TypeError(f"{value=} is not JSON-compatible")

        self.pre_recursion_init(
            **pre_recursion_args,
        )

        if instantiate_mapping:
            self.data = self.instantiate_mapping(value)
        elif instantiate_sequence:
            self.data = self.instantiate_sequence(value)

    def pre_recursion_init(self, **kwargs: Any) -> None:
        """
        Initialization code run between parent and child initialization.

        Subclasses that need to run code after the basic attributes of
        this node and its parant nodes have been initialized, but before
        child nodes are initialized, should override this method and
        pass its arguments trhough the ``pre_recursion_args`` parameter
        to the constructor.
        """
        pass

    def instantiate_sequence(
        self,
        value: Sequence[JSONCompatible],
    ) -> Sequence[JSON]:
        """Recursively instantiate JSON arrays.

        By default, instantiate elements as :attr:`itemclass` instances,
        passing :attr:`itemkwargs` in addition to the parent and key.
        """
        return [
            self.itemclass(v, parent=self, key=str(i), **self.itemkwargs)
            for i, v in enumerate(value)
        ]

    def instantiate_mapping(
        self,
        value: Mapping[JSONCompatible],
    ) -> Mapping[JSON]:
        """Recursively instantiate JSON objects.

        By default, instantiate elements as :attr:`itemclass` instances,
        passing :attr:`itemkwargs` in addition to the parent and key.
        """
        return {
            k: self.itemclass(v, parent=self, key=k, **self.itemkwargs)
            for k, v in value.items()
        }

    @cached_property
    def path(self) -> JSONPointer:
        """Return the path to the instance from the document root."""
        keys = deque()
        node = self
        while node.parent is not None:
            keys.appendleft(node.key)
            node = node.parent
        return JSONPointer(keys)

    @cached_property
    def value(self) -> JSONCompatible:
        """Return the instance data as a JSON-compatible Python object."""
        if isinstance(self.data, list):
            return [item.value for item in self.data]
        if isinstance(self.data, dict):
            return {key: item.value for key, item in self.data.items()}
        return self.data

    @cached_property
    def document_root(self) -> JSON:
        return self if self.parent is None else self.parent.document_root

    def _invalidate_path(self) -> None:
        try:
            del self.path
        except AttributeError:
            pass
        if self.type == 'array':
            for item in self.data:
                item._invalidate_path()
        elif self.type == 'object':
            for item in self.data.values():
                item._invalidate_path()

    def _invalidate_value(self) -> None:
        try:
            del self.value
        except AttributeError:
            pass
        if self.parent is not None:
            self.parent._invalidate_value()

    def dumpf(self, path: Union[str, PathLike]) -> None:
        """Serialize the instance data to a JSON file.

        :param path: the path to the file
        """
        json_dumpf(self.data, path)

    def dumps(self) -> str:
        """Serialize the instance data to a JSON string."""
        return json_dumps(self.data)

    def __repr__(self) -> str:
        """Return `repr(self)`."""
        return f'{self.__class__.__name__}({json.loads(self.dumps())!r})'

    def __str__(self) -> str:
        """Return `str(self)`."""
        return self.dumps()

    def __bool__(self) -> bool:
        """Return `bool(self)`."""
        return bool(self.data)

    def __len__(self) -> int:
        """Return `len(self)`.

        Supported for JSON types ``string``, ``array`` and ``object``.
        """
        return len(self.data)

    def __iter__(self) -> Iterator:
        """Return `iter(self)`.

        Supported for JSON types ``array`` and ``object``.
        """
        return iter(self.data)

    def __getitem__(self, index: Union[int, slice, str]) -> JSON:
        """Return `self[index]`.

        Supported for JSON types ``array`` and ``object``.
        """
        return self.data[index]

    def __setitem__(self, index: Union[int, str], obj: Union[JSON, JSONCompatible]) -> None:
        """Set `self[index]` to `obj`.

        Supported for JSON types ``array`` and ``object``.
        """
        self.data[index] = self.itemclass(
            obj.value if isinstance(obj, JSON) else obj,
            parent=self,
            key=str(index),
            **self.itemkwargs,
        )
        self._invalidate_value()

    def __delitem__(self, index: Union[int, str]) -> None:
        """Delete `self[index]`.

        Supported for JSON types ``array`` and ``object``.
        """
        del self.data[index]
        self._invalidate_value()
        if self.type == 'array':
            for item in self.data[index:]:
                item.key = str(int(item.key) - 1)
                item._invalidate_path()

    def insert(self, index: int, obj: Union[JSON, JSONCompatible]) -> None:
        """Insert `obj` before `index`.

        Supported for JSON type ``array``.
        """
        self.data.insert(index, self.itemclass(
            obj.value if isinstance(obj, JSON) else obj,
            parent=self,
            key=str(index),
            **self.itemkwargs,
        ))
        self._invalidate_value()
        for item in self.data[index + 1:]:
            item.key = str(int(item.key) + 1)
            item._invalidate_path()

    def __eq__(self, other: Union[JSON, JSONCompatible]) -> bool:
        """Return `self == other`."""
        if not isinstance(other, JSON):
            other = JSON(other)
        if self.type == other.type:
            if self.type == "array":
                return len(self) == len(other) and \
                       all(item == other[i] for i, item in enumerate(self))
            if self.type == "object":
                return self.keys() == other.keys() and \
                       all(item == other[k] for k, item in self.items())
            return self.data == other.data
        return NotImplemented

    def __ge__(self, other: Union[JSON, int, float, str]) -> bool:
        """Return `self >= other`.

        Supported for JSON types ``number`` and ``string``.
        """
        if isinstance(other, JSON):
            return self.data >= other.data
        return self.data >= other

    def __gt__(self, other: Union[JSON, int, float, str]) -> bool:
        """Return `self > other`.

        Supported for JSON types ``number`` and ``string``.
        """
        if isinstance(other, JSON):
            return self.data > other.data
        return self.data > other

    def __le__(self, other: Union[JSON, int, float, str]) -> bool:
        """Return `self <= other`.

        Supported for JSON types ``number`` and ``string``.
        """
        if isinstance(other, JSON):
            return self.data <= other.data
        return self.data <= other

    def __lt__(self, other: Union[JSON, int, float, str]) -> bool:
        """Return `self < other`.

        Supported for JSON types ``number`` and ``string``.
        """
        if isinstance(other, JSON):
            return self.data < other.data
        return self.data < other

    def add(self, path: Union[str, JSONPointer], obj: Union[JSON, JSONCompatible]) -> None:
        """Add `obj` at `path` relative to `self`.

        The :class:`JSON` equivalent to :func:`~jschon.jsonpatch.add`,
        this method performs an in-place JSON Patch ``add`` operation on `self`.

        If `path` is empty, the value of `self` is replaced by `obj`.

        *Experimental.*
        """
        if not path:
            self.__init__(
                obj.value if isinstance(obj, JSON) else obj,
                parent=self.parent,
                key=self.key,
                itemclass=self.itemclass,
                **self.itemkwargs,
            )
            self._invalidate_value()
            return

        if not isinstance(path, JSONPointer):
            path = JSONPointer(path)

        try:
            target_parent: JSON = path[:-1].evaluate(self)
            target_key = path[-1]
        except JSONPointerError as e:
            raise JSONError(f"Parent node must exist at '{path[:-1]}'") from e

        if target_parent.type == 'array':
            try:
                if target_key == '-' or int(target_key) == len(target_parent):
                    target_index = len(target_parent)
                elif 0 <= int(target_key) < len(target_parent):
                    target_index = int(target_key)
                else:
                    raise ValueError
            except ValueError:
                raise JSONError(f'Invalid array index {target_key}')

            target_parent.insert(target_index, obj)

        elif target_parent.type == 'object':
            target_parent[target_key] = obj

        else:
            raise JSONError(f"Expecting an array or object at '{target_parent.path}'")

    def remove(self, path: Union[str, JSONPointer]) -> None:
        """Remove the instance at `path` relative to `self`.

        The :class:`JSON` equivalent to :func:`~jschon.jsonpatch.remove`,
        this method performs an in-place JSON Patch ``remove`` operation on `self`.

        If `path` is empty, the value of `self` is set to `None`.

        *Experimental.*
        """
        if not path:
            self.__init__(
                None,
                parent=self.parent,
                key=self.key,
                itemclass=self.itemclass,
                **self.itemkwargs,
            )
            self._invalidate_value()
            return

        if not isinstance(path, JSONPointer):
            path = JSONPointer(path)

        try:
            target: JSON = path.evaluate(self)
        except JSONPointerError as e:
            raise JSONError(f"Target must exist at '{path}'") from e

        if target.parent.type == 'array':
            del target.parent[int(target.key)]

        elif target.parent.type == 'object':
            del target.parent[target.key]

    def replace(self, path: Union[str, JSONPointer], obj: Union[JSON, JSONCompatible]) -> None:
        """Set `obj` at `path` relative to `self`.

        The :class:`JSON` equivalent to :func:`~jschon.jsonpatch.replace`,
        this method performs an in-place JSON Patch ``replace`` operation on `self`.

        If `path` is empty, the value of `self` is replaced by `obj`.

        *Experimental.*
        """
        if not path:
            self.__init__(
                obj.value if isinstance(obj, JSON) else obj,
                parent=self.parent,
                key=self.key,
                itemclass=self.itemclass,
                **self.itemkwargs,
            )
            self._invalidate_value()
            return

        if not isinstance(path, JSONPointer):
            path = JSONPointer(path)

        try:
            target: JSON = path.evaluate(self)
        except JSONPointerError as e:
            raise JSONError(f"Target must exist at '{path}'") from e

        if target.parent.type == 'array':
            target.parent[int(target.key)] = obj

        elif target.parent.type == 'object':
            target.parent[target.key] = obj

    def move(self, from_: Union[str, JSONPointer], to: Union[str, JSONPointer]) -> None:
        """
        *Not yet implemented; experimental.*
        """

    def copy(self, from_: Union[str, JSONPointer], to: Union[str, JSONPointer]) -> None:
        """
        *Not yet implemented; experimental.*
        """

    def test(self, path: Union[str, JSONPointer], obj: Union[JSON, JSONCompatible]) -> None:
        """
        *Not yet implemented; experimental.*
        """
