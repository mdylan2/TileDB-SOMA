"""Abstractions to more easily manage read and write access to TileDB data.

``ArrayWrapper.open`` and ``GroupWrapper.open`` are the two things most important
to callers.
"""

import abc
from typing import (
    Any,
    Dict,
    Generic,
    Iterator,
    MutableMapping,
    Type,
    TypeVar,
    Union,
)

import attrs
import tiledb
from somacore import options

from .exception import DoesNotExistError, SOMAError
from .options import SOMATileDBContext
from .util_tiledb import is_does_not_exist_error

RawHandle = Union[tiledb.Array, tiledb.Group]
_RawHdl_co = TypeVar("_RawHdl_co", bound=RawHandle, covariant=True)
"""A raw TileDB object. Covariant because Handles are immutable enough."""
_Self = TypeVar("_Self", bound="AnyWrapper")


@attrs.define(eq=False, hash=False, slots=False)
class Wrapper(Generic[_RawHdl_co], metaclass=abc.ABCMeta):
    """Wrapper for TileDB handles to manage lifecycle and metadata.

    Callers may read and use (non-underscored) members but should never set
    attributes on instances.
    """

    uri: str
    mode: options.OpenMode
    context: SOMATileDBContext
    _handle: _RawHdl_co
    closed: bool = attrs.field(default=False, init=False)

    @classmethod
    def open(
        cls: Type[_Self], uri: str, mode: options.OpenMode, context: SOMATileDBContext
    ) -> _Self:
        if mode not in ("r", "w"):
            raise ValueError(f"Invalid open mode {mode!r}")
        try:
            tdb = cls._opener(uri, mode, context)
            handle = cls(uri, mode, context, tdb)
            if mode == "w":
                with cls._opener(uri, "r", context) as auxiliary_reader:
                    handle._do_initial_reads(auxiliary_reader)
            else:
                handle._do_initial_reads(tdb)
        except tiledb.TileDBError as tdbe:
            if is_does_not_exist_error(tdbe):
                raise DoesNotExistError(f"{uri!r} does not exist")
            raise
        return handle

    @classmethod
    @abc.abstractmethod
    def _opener(
        cls, uri: str, mode: options.OpenMode, context: SOMATileDBContext
    ) -> _RawHdl_co:
        """Opens and returns a TileDB object specific to this type."""
        raise NotImplementedError()

    # Covariant types should normally not be in parameters, but this is for
    # internal use only so it's OK.
    def _do_initial_reads(self, reader: _RawHdl_co) -> None:  # type: ignore[misc]
        """Final setup step before returning the Handle.

        This is passed a raw TileDB object opened in read mode, since writers
        will need to retrieve data from the backing store on setup.
        """
        self.metadata = MetadataWrapper(self, dict(reader.meta))

    @property
    def reader(self) -> _RawHdl_co:
        """Accessor to assert that you are working in read mode."""
        if self.mode == "r":
            return self._handle
        raise SOMAError(f"cannot read {self.uri!r}; it is open for writing")

    @property
    def writer(self) -> _RawHdl_co:
        """Accessor to assert that you are working in write mode."""
        if self.mode == "w":
            return self._handle
        raise SOMAError(f"cannot write {self.uri!r}; it is open for reading")

    def close(self) -> None:
        if self.closed:
            return
        self._handle.close()
        self.closed = True

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.mode} on {self.uri!r}>"

    def __enter__(self: _Self) -> _Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


AnyWrapper = Wrapper[RawHandle]
"""Non-instantiable type representing any Handle."""


class ArrayWrapper(Wrapper[tiledb.Array]):
    @classmethod
    def _opener(
        cls, uri: str, mode: options.OpenMode, context: SOMATileDBContext
    ) -> tiledb.Array:
        return tiledb.open(uri, mode, ctx=context.tiledb_ctx)

    @property
    def schema(self) -> tiledb.ArraySchema:
        return self._handle.schema


@attrs.define(frozen=True)
class GroupEntry:
    uri: str
    typ: Type[RawHandle]

    @classmethod
    def from_object(cls, obj: tiledb.Object) -> "GroupEntry":
        return GroupEntry(obj.uri, obj.type)


class GroupWrapper(Wrapper[tiledb.Group]):
    @classmethod
    def _opener(
        cls, uri: str, mode: options.OpenMode, context: SOMATileDBContext
    ) -> tiledb.Group:
        return tiledb.Group(uri, mode, ctx=context.tiledb_ctx)

    def _do_initial_reads(self, reader: tiledb.Group) -> None:
        super()._do_initial_reads(reader)
        self.initial_contents = {
            o.name: GroupEntry.from_object(o) for o in reader if o.name is not None
        }

    def _flush_hack(self) -> None:
        """On write handles, flushes pending writes. Does nothing to reads."""
        if self.mode == "w":
            self._handle.close()
            self._handle = self._opener(self.uri, "w", self.context)


@attrs.define(frozen=True)
class MetadataWrapper(MutableMapping[str, Any]):
    """A wrapper storing the metadata of some TileDB object.

    Because the view of metadata does not change after open time, we immediately
    cache all of it and use that to handle all reads. Writes are then proxied
    through to the backing store and the cache is updated to match.
    """

    owner: Wrapper[RawHandle]
    cache: Dict[str, Any]

    def __len__(self) -> int:
        return len(self.cache)

    def __iter__(self) -> Iterator[str]:
        return iter(self.cache)

    def __getitem__(self, key: str) -> Any:
        return self.cache[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.owner.writer.meta[key] = value
        self.cache[key] = value

    def __delitem__(self, key: str) -> None:
        del self.owner.writer.meta[key]
        del self.cache[key]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.owner})"
