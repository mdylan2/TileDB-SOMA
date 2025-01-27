from __future__ import annotations

import itertools
import re
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
    overload,
)

import attrs
import somacore
import somacore.collection
import tiledb
from somacore import options
from typing_extensions import Self

from . import _funcs, _tdb_handles
from ._common_nd_array import NDArray
from ._dataframe import DataFrame
from ._dense_nd_array import DenseNDArray
from ._exception import SOMAError, is_does_not_exist_error
from ._sparse_nd_array import SparseNDArray
from ._tiledb_object import AnyTileDBObject, TileDBObject
from ._util import is_relative_uri, make_relative_path, uri_joinpath
from .options import SOMATileDBContext

# A collection can hold any sub-type of TileDBObject
CollectionElementType = TypeVar("CollectionElementType", bound=AnyTileDBObject)
_TDBO = TypeVar("_TDBO", bound=AnyTileDBObject)
_Coll = TypeVar("_Coll", bound="CollectionBase[AnyTileDBObject]")
_NDArr = TypeVar("_NDArr", bound=NDArray)


@attrs.define()
class _CachedElement:
    """Item we have loaded in the cache of a collection."""

    entry: _tdb_handles.GroupEntry
    soma: Optional[AnyTileDBObject] = None
    """The reified object, if it has been opened."""


class CollectionBase(
    TileDBObject[_tdb_handles.GroupWrapper],
    somacore.collection.BaseCollection[CollectionElementType],
):
    """
    Contains a key-value mapping where the keys are string names and the values
    are any SOMA-defined foundational or composed type, including ``Collection``,
    ``DataFrame``, ``DenseNDArray``, ``SparseNDArray`` or ``Experiment``.
    """

    __slots__ = ("_contents", "_mutated_keys")
    _wrapper_type = _tdb_handles.GroupWrapper

    # TODO: Implement additional creation of members on collection subclasses.
    @classmethod
    def create(
        cls,
        uri: str,
        *,
        platform_config: Optional[options.PlatformConfig] = None,
        context: Optional[SOMATileDBContext] = None,
    ) -> Self:
        """Creates and opens a new SOMA collection in storage.

        This creates a new SOMA collection of the current type in storage and
        returns it opened for writing.

        :param uri: The location to create this SOMA collection at.
        :param platform_config: Optional call-specific options to use when
            creating this collection. (Currently unused.)
        :param context: If provided, the ``SOMATileDBContext`` to use when creating and
            opening this collection.
        """
        context = context or SOMATileDBContext()
        tiledb.group_create(uri=uri, ctx=context.tiledb_ctx)
        handle = cls._wrapper_type.open(uri, "w", context)
        cls._set_create_metadata(handle)
        return cls(
            handle,
            _dont_call_this_use_create_or_open_instead="tiledbsoma-internal-code",
        )

    # Subclass protocol to constrain which SOMA objects types  may be set on a
    # particular collection key. Used by Experiment and Measurement.
    _subclass_constrained_soma_types: ClassVar[Dict[str, Tuple[str, ...]]] = {}
    """A map limiting what types may be set to certain keys.

    Map keys are the key of the collection to constrain; values are the SOMA
    type names of the types that may be set to the key.  See ``Experiment`` and
    ``Measurement`` for details.
    """

    def __init__(
        self,
        handle: _tdb_handles.GroupWrapper,
        **kwargs: Any,
    ):
        super().__init__(handle, **kwargs)
        self._contents = {
            key: _CachedElement(entry) for key, entry in handle.initial_contents.items()
        }
        """The contents of the persisted TileDB Group.

        This is loaded at startup when we have a read handle.
        """
        self._mutated_keys: Set[str] = set()

    # Overloads to allow type inference to work when doing:
    #
    #     some_coll.add_new_collection("key")  # -> Collection
    # and
    #     some_coll.add_new_collection("key", Experiment)  # -> Experiment
    #
    # These are only used in type inference to provide better type-checking and
    # autocompletion etc. in static analysis, not at runtime.

    @overload  # type: ignore[override]  # intentionally stricter
    def add_new_collection(
        self,
        key: str,
        cls: None = None,
        *,
        uri: Optional[str] = ...,
        platform_config: Optional[options.PlatformConfig] = ...,
    ) -> "Collection[AnyTileDBObject]":
        ...

    @overload
    def add_new_collection(
        self,
        key: str,
        cls: Type[_Coll],
        *,
        uri: Optional[str] = ...,
        platform_config: Optional[options.PlatformConfig] = ...,
    ) -> _Coll:
        ...

    def add_new_collection(
        self,
        key: str,
        cls: Optional[Type[AnyTileDBCollection]] = None,
        *,
        uri: Optional[str] = None,
        platform_config: Optional[options.PlatformConfig] = None,
    ) -> "AnyTileDBCollection":
        """Adds a new sub-collection to this collection.

        :param key: The key to add.
        :param cls: Optionally, the specific type of sub-collection to create.
            For instance, passing ``tiledbsoma.Experiment`` here will create a
            ``SOMAExperiment`` as the sub-entry. By default, a basic
            ``Collection`` will be created.
        :param uri: If provided, the sub-collection will be created at this URI.
            This can be absolute, in which case the sub-collection will be
            linked to by absolute URI in the stored collection, or relative,
            in which case the sub-collection will be linked to by relative URI.
            The default is to use a relative URI generated based on the key.
        :param platform_config: Platform configuration options to use when
            creating this sub-collection. This is passed directly to
            ``[CurrentCollectionType].create()``.
        """
        child_cls: Type[AnyTileDBCollection] = cls or Collection
        return self._add_new_element(
            key,
            child_cls,
            lambda create_uri: child_cls.create(
                create_uri, platform_config=platform_config, context=self.context
            ),
            uri,
        )

    @_funcs.forwards_kwargs_to(DataFrame.create, exclude=("context",))
    def add_new_dataframe(
        self, key: str, *, uri: Optional[str] = None, **kwargs: Any
    ) -> DataFrame:
        """Adds a new DataFrame to this collection.

        For details about the behavior of ``key`` and ``uri``, see
        :meth:`add_new_collection`. The remaining parameters are passed to
        :meth:`DataFrame.create` unchanged.
        """
        return self._add_new_element(
            key,
            DataFrame,
            lambda create_uri: DataFrame.create(
                create_uri,
                context=self.context,
                **kwargs,
            ),
            uri,
        )

    @_funcs.forwards_kwargs_to(NDArray.create, exclude=("context",))
    def _add_new_ndarray(
        self, cls: Type[_NDArr], key: str, *, uri: Optional[str] = None, **kwargs: Any
    ) -> _NDArr:
        """Internal implementation of common NDArray-adding operations."""
        return self._add_new_element(
            key,
            cls,
            lambda create_uri: cls.create(
                create_uri,
                context=self.context,
                **kwargs,
            ),
            uri,
        )

    @_funcs.forwards_kwargs_to(_add_new_ndarray, exclude=("cls",))
    def add_new_dense_ndarray(self, key: str, **kwargs: Any) -> DenseNDArray:
        """Adds a new DenseNDArray to this Collection.

        For details about the behavior of ``key`` and ``uri``, see
        :meth:`add_new_collection`. The remaining parameters are passed to
        the :meth:`DenseNDArray.create` method unchanged.
        """
        return self._add_new_ndarray(DenseNDArray, key, **kwargs)

    @_funcs.forwards_kwargs_to(_add_new_ndarray, exclude=("cls",))
    def add_new_sparse_ndarray(self, key: str, **kwargs: Any) -> SparseNDArray:
        """Adds a new SparseNDArray to this Collection.

        For details about the behavior of ``key`` and ``uri``, see
        :meth:`add_new_collection`. The remaining parameters are passed to
        the :meth:`SparseNDArray.create` method unchanged.
        """
        return self._add_new_ndarray(SparseNDArray, key, **kwargs)

    def _add_new_element(
        self,
        key: str,
        cls: Type[_TDBO],
        factory: Callable[[str], _TDBO],
        user_uri: Optional[str],
    ) -> _TDBO:
        """Handles the common parts of adding new elements.

        :param key: The key to be added.
        :param cls: The type of the element to be added.
        :param factory: A callable that, given the full URI to be added,
            will create the backing storage at that URI and return
            the reified SOMA object.
        :param user_uri: If set, the URI to use for the child
            instead of the default.
        """
        if key in self:
            raise KeyError(f"{key!r} already exists in {type(self)}")
        self._check_allows_child(key, cls)
        child_uri = self._new_child_uri(key=key, user_uri=user_uri)
        child = factory(child_uri.full_uri)
        # The resulting element may not be the right type for this collection,
        # but we can't really handle that within the type system.
        self._set_element(
            key,
            uri=child_uri.add_uri,
            relative=child_uri.relative,
            soma_object=child,  # type: ignore[arg-type]
        )
        self._close_stack.enter_context(child)
        return child

    def __len__(self) -> int:
        """
        Return the number of members in the collection
        """
        return len(self._contents)

    def __getitem__(self, key: str) -> CollectionElementType:
        """
        Gets the value associated with the key.
        """

        err_str = f"{self.__class__.__name__} has no item {key!r}"

        try:
            entry = self._contents[key]
        except KeyError:
            raise KeyError(err_str) from None
        if entry.soma is None:
            from . import _factory  # Delayed binding to resolve circular import.

            entry.soma = _factory._open_internal(
                entry.entry.wrapper_type.open,
                entry.entry.uri,
                self.mode,
                self.context,
            )
            # Since we just opened this object, we own it and should close it.
            self._close_stack.enter_context(entry.soma)
        return cast(CollectionElementType, entry.soma)

    def set(
        self,
        key: str,
        value: CollectionElementType,
        *,
        use_relative_uri: Optional[bool] = None,
    ) -> Self:
        """Adds an element to the collection. [lifecycle: experimental]

        :param key: The key of the element to be added.
        :param value: The value to be added to this collection.
        :param use_relative_uri: By default (None), the collection will
            determine whether the element should be stored by relative URI.
            If True, the collection will store the child by absolute URI.
            If False, the collection will store the child by relative URI.
        """
        uri_to_add = value.uri
        # The SOMA API supports use_relative_uri in [True, False, None].
        # The TileDB-Py API supports use_relative_uri in [True, False].
        # Map from the former to the latter -- and also honor our somacore contract for None --
        # using the following rule.
        if use_relative_uri is None and value.uri.startswith("tiledb://"):
            # TileDB-Cloud does not use relative URIs, ever.
            use_relative_uri = False

        if use_relative_uri is not False:
            try:
                uri_to_add = make_relative_path(value.uri, relative_to=self.uri)
                use_relative_uri = True
            except ValueError:
                if use_relative_uri:
                    # We couldn't construct a relative URI, but we were asked
                    # to use one, so raise the error.
                    raise
                use_relative_uri = False

        self._set_element(
            key, uri=uri_to_add, relative=use_relative_uri, soma_object=value
        )
        return self

    def __setitem__(self, key: str, value: CollectionElementType) -> None:
        """
        Default collection __setattr__
        """
        self.set(key, value, use_relative_uri=None)

    def __delitem__(self, key: str) -> None:
        """
        Removes a member from the collection, when invoked as ``del collection["namegoeshere"]``.
        """
        self._del_element(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._contents)

    def __repr__(self) -> str:
        """
        Default display for ``Collection``.
        """
        lines = itertools.chain((self._my_repr(),), self._contents_lines(""))
        return "<" + "\n".join(lines) + ">"

    # ================================================================
    # PRIVATE METHODS FROM HERE ON DOWN
    # ================================================================

    def _my_repr(self) -> str:
        start = super()._my_repr()
        if self.closed:
            return start
        n = len(self)
        if n == 0:
            count = "empty"
        elif n == 1:
            count = "1 item"
        else:
            count = f"{n} items"
        return f"{start} ({count})"

    def _contents_lines(self, last_indent: str) -> Iterable[str]:
        indent = last_indent + "    "
        if self.closed:
            return ()
        for key, entry in self._contents.items():
            obj = entry.soma
            if obj is None:
                # We haven't reified this SOMA object yet. Don't try to open it.
                yield f"{indent}{key!r}: {entry.entry.uri!r} (unopened)"
            else:
                yield f"{indent}{key!r}: {obj._my_repr()}"
                if isinstance(obj, CollectionBase):
                    yield from obj._contents_lines(indent)

    def _set_element(
        self,
        key: str,
        *,
        uri: str,
        relative: bool,
        soma_object: CollectionElementType,
    ) -> None:
        """Internal implementation of element setting.

        :param key: The key to set.
        :param uri: The resolved URI to pass to :meth:`tiledb.Group.add`.
        :param relative: The ``relative`` parameter to pass to ``add``.
        :param value: The reified SOMA object to store locally.
        """

        self._check_allows_child(key, type(soma_object))

        if key in self._mutated_keys.union(self._contents):
            # TileDB groups currently do not support replacing elements.
            # If we use a hack to flush writes, corruption is possible.
            raise SOMAError(f"replacing key {key!r} is unsupported")
        self._handle.writer.add(name=key, uri=uri, relative=relative)
        self._contents[key] = _CachedElement(
            entry=_tdb_handles.GroupEntry(soma_object.uri, soma_object._wrapper_type),
            soma=soma_object,
        )
        self._mutated_keys.add(key)

    def _del_element(self, key: str) -> None:
        if key in self._mutated_keys:
            raise SOMAError(f"cannot delete previously-mutated key {key!r}")
        try:
            self._handle.writer.remove(key)
        except tiledb.TileDBError as tdbe:
            if is_does_not_exist_error(tdbe):
                raise KeyError(f"{key!r} does not exist in {self}") from tdbe
            raise
        self._contents.pop(key, None)
        self._mutated_keys.add(key)

    def _new_child_uri(self, *, key: str, user_uri: Optional[str]) -> "_ChildURI":
        maybe_relative_uri = user_uri or _sanitize_for_path(key)
        if not is_relative_uri(maybe_relative_uri):
            # It's an absolute URI.
            return _ChildURI(
                add_uri=maybe_relative_uri,
                full_uri=maybe_relative_uri,
                relative=False,
            )
        if not self.uri.startswith("tiledb://"):
            # We don't need to post-process anything.
            return _ChildURI(
                add_uri=maybe_relative_uri,
                full_uri=uri_joinpath(self.uri, maybe_relative_uri),
                relative=True,
            )
        # Our own URI is a `tiledb://` URI. Since TileDB Cloud requires absolute
        # URIs, we need to calculate the absolute URI to pass to Group.add
        # based on our creation URI.
        # TODO: Handle the case where we reopen a TileDB Cloud Group, but by
        # name rather than creation path.
        absolute_uri = uri_joinpath(self.uri, maybe_relative_uri)
        return _ChildURI(add_uri=absolute_uri, full_uri=absolute_uri, relative=False)

    @classmethod
    def _check_allows_child(cls, key: str, child_cls: type) -> None:
        real_child = _real_class(child_cls)
        if not issubclass(real_child, TileDBObject):
            raise TypeError(
                f"only TileDB objects can be added as children of {cls}, not {child_cls}"
            )
        constraint = cls._subclass_constrained_soma_types.get(key)
        if constraint is not None and real_child.soma_type not in constraint:
            raise TypeError(
                f"cannot add {child_cls} at {cls}[{key!r}]; only {constraint}"
            )


AnyTileDBCollection = CollectionBase[Any]


class Collection(
    CollectionBase[CollectionElementType], somacore.Collection[CollectionElementType]
):
    """
    A persistent collection of SOMA objects, mapping string keys to any SOMA object.

    [lifecycle: experimental]
    """

    __slots__ = ()


def _real_class(cls: Type[Any]) -> type:
    """Extracts the real class from a generic alias.

    Generic aliases like ``Collection[whatever]`` cannot be used in instance or
    subclass checks because they are not actual types present at runtime.
    This extracts the real type from a generic alias::

        _real_class(Collection[whatever])  # -> Collection
        _real_class(List[whatever])  # -> list
    """
    try:
        # If this is a generic alias (e.g. List[x] or list[x]), this will fail.
        issubclass(object, cls)  # Ordering intentional here.
        # Do some extra checking because later Pythons get weird.
        if issubclass(cls, object) and isinstance(cls, type):
            return cls
    except TypeError:
        pass
    err = TypeError(f"{cls} cannot be turned into a real type")
    try:
        # All types of generic alias have this.
        origin = getattr(cls, "__origin__")
        # Other special forms, like Union, also have an __origin__ that is not
        # an actual type.  Verify that the origin is a real, instantiable type.
        issubclass(object, origin)  # Ordering intentional here.
        if issubclass(origin, object) and isinstance(origin, type):
            return origin
    except (AttributeError, TypeError) as exc:
        raise err from exc
    raise err


_NON_WORDS = re.compile(r"[\W_]+")


def _sanitize_for_path(key: str) -> str:
    """Prepares the given key for use as a path component."""
    sanitized = "_".join(_NON_WORDS.split(key))
    return sanitized


@attrs.define(frozen=True, kw_only=True)
class _ChildURI:
    add_uri: str
    """The URI of the child for passing to :meth:`tiledb.Group.add`."""
    full_uri: str
    """The full URI of the child, used to create a new element."""
    relative: bool
    """The ``relative`` value to pass to :meth:`tiledb.Group.add`."""
