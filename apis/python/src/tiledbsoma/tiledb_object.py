from contextlib import ExitStack
from typing import Any, ClassVar, Generic, MutableMapping, Type, TypeVar

import somacore
import tiledb
from somacore import options

from . import constants, handles
from .options import SOMATileDBContext
from .types import StorageType

_HandleType_co = TypeVar("_HandleType_co", bound=handles.AnyWrapper, covariant=True)
"""The type of handle on a backend object that we have.

Covariant because ``_handle`` is read-only.
"""


class TileDBObject(somacore.SOMAObject, Generic[_HandleType_co]):
    """
    Base class for ``TileDBArray`` and ``Collection``.

    Accepts a SOMATileDBContext, to enable session state to be shared across SOMA objects.

    [lifecycle: experimental]
    """

    __slots__ = ("_handle", "_close_stack", "_closed")

    def __init__(
        self,
        handle: _HandleType_co,
        *,
        _this_is_internal_only: str = "unset",
    ):
        """Common initialization.

        This function is internal; users should open TileDB SOMA object using
        the :meth:`create` and :meth:`open` factory class methods.
        """
        if _this_is_internal_only != "tiledbsoma-internal-code":
            name = type(self).__name__
            raise TypeError(
                f"{name} initializers are intended for internal use only."
                f" To open an existing {name}, use tiledbsoma.open(...)"
                f" or the {name}.open(...) class method."
                f" To create a new {name}, use the {name}.create class method."
            )
        self._handle = handle
        self._close_stack = ExitStack()
        """An exit stack to manage closing handles owned by this object.

        This is used to manage both our direct handle (in the case of simple
        TileDB objects) and the lifecycle of owned children (in the case of
        Collections).
        """
        self._close_stack.enter_context(self._handle)
        self._closed = False

    _STORAGE_TYPE: StorageType
    _tiledb_type: ClassVar[Type[handles.RawHandle]]

    @property
    def context(self) -> SOMATileDBContext:
        return self._handle.context

    @property
    def _ctx(self) -> tiledb.Ctx:
        return self.context.tiledb_ctx

    @property
    def metadata(self) -> MutableMapping[str, Any]:
        return self._handle.metadata

    def __repr__(self) -> str:
        return f'{self.soma_type}(uri="{self.uri}")'

    @property
    def uri(self) -> str:
        """
        Accessor for the object's storage URI
        """
        return self._handle.uri

    def close(self) -> None:
        """
        Release any resources held while the object is open. Closing an already-closed object is a
        no-op.
        """
        self._close_stack.close()
        self._closed = True

    @property
    def closed(self) -> bool:
        """True if the object has been closed. False if it is still open."""
        return self._closed

    @property
    def mode(self) -> options.OpenMode:
        """
        Current open mode: read (r), write (w), or closed (None).
        """
        return self._handle.mode

    @classmethod
    def _set_create_metadata(cls, handle: handles.AnyWrapper) -> None:
        """Sets the necessary metadata on a newly-created TileDB object."""
        handle.writer.meta.update(
            {
                constants.SOMA_OBJECT_TYPE_METADATA_KEY: cls.soma_type,
                constants.SOMA_ENCODING_VERSION_METADATA_KEY: constants.SOMA_ENCODING_VERSION,
            }
        )

    def _check_open_read(self) -> None:
        if self.mode != "r":
            raise ValueError(f"{self} is open for writing, not reading")


AnyTileDBObject = TileDBObject[handles.AnyWrapper]
