from typing import Optional

import pyarrow as pa
import somacore
from somacore import options
from typing_extensions import Self

from . import _util
from ._common_nd_array import NDArray
from ._exception import SOMAError
from ._util import dense_indices_to_shape


class DenseNDArray(NDArray, somacore.DenseNDArray):
    """
    ``DenseNDArray`` is a dense, N-dimensional array, with offset (zero-based)
    integer indexing on each dimension. ``DenseNDArray`` has a user-defined
    schema, which includes:
    - the element type, expressed as an Arrow type, indicating the type of data
      contained within the array, and
    - the shape of the array, i.e., the number of dimensions and the length of
      each dimension

    All dimensions must have a positive, non-zero length, and there must be 1
    or more dimensions.

    Where explicitly referenced in the API, the dimensions are named
    ``soma_dim_N``, where N is the dimension number (eg, ``soma_dim_0``),
    and elements are named ``soma_data``.

    [lifecycle: experimental]

    Examples:
    ---------
    >>> import tiledbsoma
    >>> import pyarrow as pa
    >>> import numpy as np
    >>> with tiledbsoma.DenseNDArray.create(
    ...     "./test_dense_ndarray", type=pa.int32(), shape=(2, 3, 4)
    ... ) as arr:
    ...     data = pa.Tensor.from_numpy(
    ...         np.random.default_rng().integers(0, 10, 24).reshape(2, 3, 4)
    ...     )
    ...     arr.write((slice(None),), data)
    ... with tiledbsoma.open("./test_dense_ndarray") as arr:
    ...     print(arr.schema)
    ...     print("---")
    ...     print(arr.read())
    ...
    soma_dim_0: int64
    soma_dim_1: int64
    soma_dim_2: int64
    soma_data: int32
    ---
    <pyarrow.Tensor>
    type: int32
    shape: (2, 3, 4)
    strides: (48, 16, 4)
    """

    __slots__ = ()

    def read(
        self,
        coords: options.DenseNDCoords = (),
        *,
        result_order: options.ResultOrderStr = somacore.ResultOrder.ROW_MAJOR,
        partitions: Optional[options.ReadPartitions] = None,
        platform_config: Optional[options.PlatformConfig] = None,
    ) -> pa.Tensor:
        """
        Read a user-defined dense slice of the array and return as an Arrow ``Tensor``.

        Coordinates must specify a contiguous subarray, and the number of coordinates
        must be less than or equal to the number of dimensions. For example,
        if the array is 10 by 20, then some acceptable values of ``coords`` include
        ``(3, 4)``, ``(slice(5, 10),)``, and ``(slice(5, 10), slice(6, 12))``.
        Slice indices are doubly inclusive.

        [lifecycle: experimental]
        """
        del partitions, platform_config  # Currently unused.
        self._check_open_read()
        result_order = somacore.ResultOrder(result_order)

        schema = self._handle.schema
        target_shape = dense_indices_to_shape(coords, schema.shape, result_order)

        sr = self._soma_reader(result_order=result_order.value)

        self._set_reader_coords(sr, coords)

        sr.submit()

        arrow_tables = []
        while True:
            arrow_table_piece = sr.read_next()
            if not arrow_table_piece:
                break
            arrow_tables.append(arrow_table_piece)

        # For dense arrays there is no zero-output case: attempting to make a test case
        # to do that, say by indexing a 10x20 array by positions 888 and 999, results
        # in read-time errors of the form
        #
        # [TileDB::Subarray] Error: Cannot add range to dimension 'soma_dim_0'; Range [888, 888] is
        # out of domain bounds [0, 9]
        if not arrow_tables:
            raise SOMAError(
                "internal error: at least one table-piece should have been returned"
            )

        arrow_table = pa.concat_tables(arrow_tables)
        return pa.Tensor.from_numpy(
            arrow_table.column("soma_data").to_numpy().reshape(target_shape)
        )

    def write(
        self,
        coords: options.DenseNDCoords,
        values: pa.Tensor,
        *,
        platform_config: Optional[options.PlatformConfig] = None,
    ) -> Self:
        """
        Write subarray, defined by ``coords`` and ``values``. Will overwrite existing
        values in the array.

        [lifecycle: experimental]

        :param coords: A per-dimension tuple of scalars or slices
            defining the bounds of the subarray to be written.
        :param values: The values to be written to the subarray.  Must have
        the same shape as ``coords``, and the type must match the DenseNDArray.
        :param platform_config: Optional platform-specific options to use
            in this write operation (currently unused).
        """
        _util.check_type("values", values, (pa.Tensor,))

        del platform_config  # Currently unused.
        self._handle.writer[coords] = values.to_numpy()
        return self
