import math
import time
from typing import (
    Any,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import anndata as ad
import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import scipy.sparse as sp
from anndata._core.sparse_dataset import SparseDataset
from somacore.options import PlatformConfig

from . import (
    Collection,
    DataFrame,
    DenseNDArray,
    Experiment,
    Measurement,
    SparseNDArray,
    eta,
    logging,
    util,
    util_ann,
    util_scipy,
    util_tiledb,
)
from .collection import AnyTileDBCollection, CollectionBase
from .common_nd_array import NDArray
from .constants import SOMA_JOINID
from .exception import DoesNotExistError, SOMAError
from .options import SOMATileDBContext
from .options.tiledb_create_options import TileDBCreateOptions
from .tdb_handles import RawHandle
from .tiledb_array import TileDBArray
from .tiledb_object import AnyTileDBObject, TileDBObject
from .types import INGEST_MODES, IngestMode, NPNDArray, Path

SparseMatrix = Union[sp.csr_matrix, sp.csc_matrix, SparseDataset]
Matrix = Union[NPNDArray, SparseMatrix]
_TDBO = TypeVar("_TDBO", bound=TileDBObject[RawHandle])


# ----------------------------------------------------------------
def from_h5ad(
    experiment_uri: str,
    input_path: Path,
    measurement_name: str,
    *,
    context: Optional[SOMATileDBContext] = None,
    platform_config: Optional[PlatformConfig] = None,
    ingest_mode: IngestMode = "write",
) -> Experiment:
    """
    Reads an .h5ad file and writes to a TileDB group structure.

    Returns an experiment opened for writing.

    The "write" ingest_mode (which is the default) writes all data, creating new layers if the soma already exists.

    The "resume" ingest_mode skips data writes if data are within dimension ranges of the existing soma.
    This is useful for continuing after a partial/interrupted previous upload.

    The "schema_only" ingest_mode creates groups and array schema, without writing array data.
    This is useful as a prep-step for parallel append-ingest of multiple H5ADs to a single soma.

    [lifecycle: experimental]
    """
    if ingest_mode not in INGEST_MODES:
        raise SOMAError(
            f'expected ingest_mode to be one of {INGEST_MODES}; got "{ingest_mode}"'
        )

    if isinstance(input_path, ad.AnnData):
        raise TypeError("Input path is an AnnData object -- did you want from_anndata?")

    s = util.get_start_stamp()
    logging.log_io(None, f"START  Experiment.from_h5ad {input_path}")

    logging.log_io(None, f"START  READING {input_path}")

    anndata = ad.read_h5ad(input_path, backed="r")

    logging.log_io(None, util.format_elapsed(s, f"FINISH READING {input_path}"))

    exp = from_anndata(
        experiment_uri,
        anndata,
        measurement_name,
        context=context,
        platform_config=platform_config,
        ingest_mode=ingest_mode,
    )

    logging.log_io(
        None, util.format_elapsed(s, f"FINISH Experiment.from_h5ad {input_path}")
    )
    return exp


# ----------------------------------------------------------------
def from_anndata(
    experiment_uri: str,
    anndata: ad.AnnData,
    measurement_name: str,
    *,
    context: Optional[SOMATileDBContext] = None,
    platform_config: Optional[PlatformConfig] = None,
    ingest_mode: IngestMode = "write",
) -> Experiment:
    """
    Top-level writer method for creating a TileDB group for a ``Experiment`` object.

    Returns an Experiment opened for writing.

    The "write" ingest_mode (which is the default) writes all data, creating new layers if the soma already exists.

    The "resume" ingest_mode skips data writes if data are within dimension ranges of the existing soma.
    This is useful for continuing after a partial/interrupted previous upload.

    The "schema_only" ingest_mode creates groups and array schema, without writing array data.
    This is useful as a prep-step for parallel append-ingest of multiple H5ADs to a single soma.

    [lifecycle: experimental]
    """
    if ingest_mode not in INGEST_MODES:
        raise SOMAError(
            f'expected ingest_mode to be one of {INGEST_MODES}; got "{ingest_mode}"'
        )

    if not isinstance(anndata, ad.AnnData):
        raise TypeError(
            "Second argument is not an AnnData object -- did you want from_h5ad?"
        )

    # Without _at least_ an index, there is nothing to indicate the dimension indices.
    if anndata.obs.index.empty or anndata.var.index.empty:
        raise NotImplementedError("Empty AnnData.obs or AnnData.var unsupported.")

    s = util.get_start_stamp()
    logging.log_io(None, "START  DECATEGORICALIZING")

    anndata.obs_names_make_unique()
    anndata.var_names_make_unique()

    logging.log_io(None, util.format_elapsed(s, "FINISH DECATEGORICALIZING"))

    s = util.get_start_stamp()
    logging.log_io(None, f"START  WRITING {experiment_uri}")

    # Must be done first, to create the parent directory.
    try:
        experiment = Experiment.open(
            experiment_uri, "w", context=context, platform_config=platform_config
        )
    except SOMAError:
        experiment = Experiment.create(
            experiment_uri, context=context, platform_config=platform_config
        )
    else:
        if ingest_mode != "resume":
            raise SOMAError(f"{experiment_uri!r} already exists")

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # OBS
    _write_dataframe(
        experiment,
        "obs",
        util_ann._decategoricalize_obs_or_var(anndata.obs),
        id_column_name="obs_id",
        platform_config=platform_config,
        ingest_mode=ingest_mode,
    )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MS
    ms = _maybe_add_collection(experiment, "ms", Collection, ingest_mode)
    measurement = _maybe_add_collection(ms, measurement_name, Measurement, ingest_mode)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MS/meas/VAR

    _write_dataframe(
        measurement,
        "var",
        util_ann._decategoricalize_obs_or_var(anndata.var),
        id_column_name="var_id",
        platform_config=platform_config,
        ingest_mode=ingest_mode,
    )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MS/meas/X/DATA

    x = _maybe_add_collection(measurement, "X", Collection, ingest_mode)

    # Since we did `anndata = ad.read_h5ad(path_to_h5ad, "r")` with the "r":
    # * If we do `anndata.X[:]` we're loading all of a CSR/CSC/etc into memory.
    # * If we do `anndata.X` we're getting a pageable object which can be loaded
    #   chunkwise into memory.
    # Using the latter allows us to ingest larger .h5ad files without OOMing.
    cls = (
        DenseNDArray
        if isinstance(anndata.X, (np.ndarray, h5py.Dataset))
        else SparseNDArray
    )
    add_ndarray_from_matrix(
        x, "data", cls, anndata.X, platform_config, ingest_mode=ingest_mode
    )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MS/meas/OBSM,VARM,OBSP,VARP
    if len(anndata.obsm.keys()) > 0:  # do not create an empty collection
        obsm = _maybe_add_collection(measurement, "obsm", Collection, ingest_mode)
        for key in anndata.obsm.keys():
            add_ndarray_from_matrix(
                obsm,
                key,
                DenseNDArray,
                util_tiledb.to_tiledb_supported_array_type(anndata.obsm[key]),
                platform_config,
                ingest_mode,
            )

    if len(anndata.varm.keys()) > 0:  # do not create an empty collection
        varm = _maybe_add_collection(measurement, "varm", Collection, ingest_mode)
        for key in anndata.varm.keys():
            add_ndarray_from_matrix(
                varm,
                key,
                DenseNDArray,
                util_tiledb.to_tiledb_supported_array_type(anndata.varm[key]),
                platform_config,
                ingest_mode,
            )

    if len(anndata.obsp.keys()) > 0:  # do not create an empty collection
        obsp = _maybe_add_collection(measurement, "obsp", Collection, ingest_mode)
        for key in anndata.obsp.keys():
            add_ndarray_from_matrix(
                obsp,
                key,
                SparseNDArray,
                util_tiledb.to_tiledb_supported_array_type(anndata.obsp[key]),
                platform_config,
                ingest_mode,
            )

    if len(anndata.varp.keys()) > 0:  # do not create an empty collection
        varp = _maybe_add_collection(measurement, "varp", Collection, ingest_mode)
        for key in anndata.varp.keys():
            add_ndarray_from_matrix(
                varp,
                key,
                SparseNDArray,
                util_tiledb.to_tiledb_supported_array_type(anndata.varp[key]),
                platform_config,
                ingest_mode,
            )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # MS/RAW
    if anndata.raw is not None:
        raw_measurement = _maybe_add_collection(ms, "raw", Measurement, ingest_mode)
        _write_dataframe(
            raw_measurement,
            "var",
            util_ann._decategoricalize_obs_or_var(anndata.raw.var),
            id_column_name="var_id",
            platform_config=platform_config,
            ingest_mode=ingest_mode,
        )

        rm_x = _maybe_add_collection(raw_measurement, "X", Collection, ingest_mode)
        add_ndarray_from_matrix(
            rm_x, "data", SparseNDArray, anndata.raw.X, platform_config, ingest_mode
        )

    logging.log_io(
        f"Wrote   {experiment.uri}",
        util.format_elapsed(s, f"FINISH WRITING {experiment.uri}"),
    )
    return experiment


_Coll = TypeVar("_Coll", bound=AnyTileDBCollection)


@util.typeguard_ignore
def _maybe_add_collection(
    parent: CollectionBase[AnyTileDBObject],
    key: str,
    cls: Type[_Coll],
    ingest_mode: IngestMode,
) -> _Coll:
    try:
        thing = cast(_Coll, parent[key])
    except KeyError:
        # This is always OK. Make a new one.
        return parent.add_new_collection(key, cls)
    if ingest_mode == "resume":
        return thing
    raise SOMAError(f"{key} already exists")


def _write_dataframe(
    parent: AnyTileDBCollection,
    key: str,
    df: pd.DataFrame,
    id_column_name: Optional[str],
    platform_config: Optional[PlatformConfig] = None,
    ingest_mode: IngestMode = "write",
) -> None:
    s = util.get_start_stamp()
    logging.log_io(None, f"START  WRITING {parent.uri}[{key}]")

    df[SOMA_JOINID] = np.asarray(range(len(df)), dtype=np.int64)

    df.reset_index(inplace=True)
    if id_column_name is not None:
        df.rename(columns={"index": id_column_name}, inplace=True)
    df.set_index(SOMA_JOINID, inplace=True)

    # Categoricals are not yet well supported, so we must flatten
    for k in df:
        if df[k].dtype == "category":
            df[k] = df[k].astype(df[k].cat.categories.dtype)
    arrow_table = pa.Table.from_pandas(df)

    try:
        soma_df = cast(DataFrame, parent[key])
    except KeyError:
        soma_df = parent.add_new_dataframe(
            key, schema=arrow_table.schema, platform_config=platform_config
        )
    else:
        if ingest_mode == "resume":
            storage_ned = _read_nonempty_domain(soma_df)
            dim_range = ((int(df.index.min()), int(df.index.max())),)
            if _chunk_is_contained_in(dim_range, storage_ned):
                logging.log_io(
                    f"Skipped {soma_df.uri}",
                    util.format_elapsed(s, f"SKIPPED {soma_df.uri}"),
                )
                return
        else:
            raise SOMAError(f"{soma_df.uri} already exists")

    if ingest_mode == "schema_only":
        logging.log_io(
            f"Wrote schema {soma_df.uri}",
            util.format_elapsed(s, f"FINISH WRITING SCHEMA {soma_df.uri}"),
        )
        return

    soma_df.write(arrow_table)
    logging.log_io(
        f"Wrote   {soma_df.uri}",
        util.format_elapsed(s, f"FINISH WRITING {soma_df.uri}"),
    )


@util.typeguard_ignore
def create_from_matrix(
    cls: Type[NDArray],
    uri: str,
    matrix: Union[Matrix, h5py.Dataset],
    platform_config: Optional[PlatformConfig] = None,
    ingest_mode: IngestMode = "write",
) -> None:
    """
    Create and populate the ``soma_matrix`` from the contents of ``matrix``.
    """
    if len(matrix.shape) != 2:
        raise ValueError(f"expected matrix.shape == 2; got {matrix.shape}")

    s = util.get_start_stamp()
    logging.log_io(None, f"START  WRITING {uri}")

    try:
        soma_ndarray = cls.open(uri, "w", platform_config=platform_config)
    except DoesNotExistError:
        soma_ndarray = cls.create(
            uri,
            type=pa.from_numpy_dtype(matrix.dtype),
            shape=matrix.shape,
            platform_config=platform_config,
        )
    else:
        if ingest_mode != "resume":
            soma_ndarray.close()
            raise SOMAError(f"{soma_ndarray.uri} already exists")
    with soma_ndarray:
        _fill_ndarray_from_matrix(soma_ndarray, matrix, platform_config, ingest_mode, s)


@util.typeguard_ignore
def add_ndarray_from_matrix(
    parent: AnyTileDBCollection,
    key: str,
    cls: Type[NDArray],
    matrix: Union[Matrix, h5py.Dataset],
    platform_config: Optional[PlatformConfig] = None,
    ingest_mode: IngestMode = "write",
) -> None:
    """
    Add the SOMA matrix to the given parent.
    """
    # SparseDataset has no ndim but it has a shape
    if len(matrix.shape) != 2:
        raise ValueError(f"expected matrix.shape == 2; got {matrix.shape}")

    s = util.get_start_stamp()
    logging.log_io(None, f"START  CREATING {parent.uri}[{key}]")

    try:
        soma_ndarray = cast(NDArray, parent[key])
    except KeyError:
        soma_ndarray = parent._add_new_ndarray(
            cls,
            key,
            type=pa.from_numpy_dtype(matrix.dtype),
            shape=matrix.shape,
            platform_config=platform_config,
        )
    else:
        if ingest_mode != "resume":
            raise SOMAError(f"{parent.uri}[{key}] already exists")

    if ingest_mode == "schema_only":
        logging.log_io(
            f"Wrote schema {soma_ndarray.uri}",
            util.format_elapsed(s, f"FINISH WRITING SCHEMA {soma_ndarray.uri}"),
        )
        return

    _fill_ndarray_from_matrix(soma_ndarray, matrix, platform_config, ingest_mode, s)


def _fill_ndarray_from_matrix(
    soma_ndarray: NDArray,
    matrix: Union[Matrix, h5py.Dataset],
    platform_config: Optional[PlatformConfig],
    ingest_mode: IngestMode,
    start_time: float,
) -> None:
    logging.log_io(
        f"Writing {soma_ndarray.uri}",
        util.format_elapsed(start_time, f"START  WRITING {soma_ndarray.uri}"),
    )

    if isinstance(soma_ndarray, DenseNDArray):
        _write_matrix_to_denseNDArray(
            soma_ndarray,
            matrix,
            tiledb_create_options=TileDBCreateOptions.from_platform_config(
                platform_config
            ),
            ingest_mode=ingest_mode,
        )
    elif isinstance(soma_ndarray, SparseNDArray):  # SOMASparseNDArray
        _write_matrix_to_sparseNDArray(
            soma_ndarray,
            matrix,
            tiledb_create_options=TileDBCreateOptions.from_platform_config(
                platform_config
            ),
            ingest_mode=ingest_mode,
        )
    else:
        raise TypeError(f"unknown array type {type(soma_ndarray)}")

    logging.log_io(
        f"Wrote   {soma_ndarray.uri}",
        util.format_elapsed(start_time, f"FINISH WRITING {soma_ndarray.uri}"),
    )


def add_X_layer(
    exp: Experiment,
    measurement_name: str,
    X_layer_name: str,
    # E.g. a scipy.csr_matrix from scanpy analysis:
    X_layer_data: Union[Matrix, h5py.Dataset],
    ingest_mode: IngestMode = "write",
) -> None:
    """
    This is useful for adding X data, for example from scanpy.pp.normalize_total, scanpy.pp.log1p, etc.

    Use `ingest_mode="resume"` to not error out if the schema already exists.

    [lifecycle: experimental]
    """
    add_matrix_to_collection(
        exp,
        measurement_name,
        "X",
        X_layer_name,
        X_layer_data,
        ingest_mode=ingest_mode,
    )


def add_matrix_to_collection(
    exp: Experiment,
    measurement_name: str,
    collection_name: str,
    matrix_name: str,
    # E.g. a scipy.csr_matrix from scanpy analysis:
    matrix_data: Union[Matrix, h5py.Dataset],
    ingest_mode: IngestMode = "write",
) -> None:
    """
    This is useful for adding X/obsp/varm/etc data, for example from scanpy.pp.normalize_total,
    scanpy.pp.log1p, etc.

    Use `ingest_mode="resume"` to not error out if the schema already exists.
    """
    meas = exp.ms[measurement_name]
    coll = _maybe_add_collection(
        meas, collection_name, Collection, ingest_mode="resume"
    )
    add_ndarray_from_matrix(
        coll, matrix_name, SparseNDArray, matrix_data, ingest_mode=ingest_mode
    )


def _write_matrix_to_denseNDArray(
    soma_ndarray: DenseNDArray,
    matrix: Union[Matrix, h5py.Dataset],
    tiledb_create_options: TileDBCreateOptions,
    ingest_mode: IngestMode,
) -> None:
    """Write a matrix to an empty DenseNDArray"""

    # There is a chunk-by-chunk already-done check for resume mode, below.
    # This full-matrix-level check here might seem redundant, but in fact it's important:
    # * By checking input bounds against storage NED here, we can see if the entire matrix
    #   was already ingested and avoid even loading chunks;
    # * By checking chunkwise we can catch the case where a matrix was already *partly*
    #   ingested.
    # * Of course, this also helps us catch already-completed writes in the non-chunked case.
    # TODO: make sure we're not using an old timestamp for this
    storage_ned = None
    if ingest_mode == "resume":
        # This lets us check for already-ingested chunks, when in resume-ingest mode.
        storage_ned = _read_nonempty_domain(soma_ndarray)
        matrix_bounds = [
            (0, int(n - 1)) for n in matrix.shape
        ]  # Cast for lint in case np.int64
        logging.log_io(
            None,
            f"Input bounds {tuple(matrix_bounds)} storage non-empty domain {storage_ned}",
        )
        if _chunk_is_contained_in(matrix_bounds, storage_ned):
            logging.log_io(
                f"Skipped {soma_ndarray.uri}", f"SKIPPED WRITING {soma_ndarray.uri}"
            )
            return

    # Write all at once?
    if not tiledb_create_options.write_X_chunked():
        if not isinstance(matrix, np.ndarray):
            matrix = matrix.toarray()
        soma_ndarray.write((slice(None),), pa.Tensor.from_numpy(matrix))
        return

    # OR, write in chunks
    eta_tracker = eta.Tracker()
    nrow, ncol = matrix.shape
    i = 0
    # Number of rows to chunk by. Dense writes, so this is a constant.
    chunk_size = int(math.ceil(tiledb_create_options.goal_chunk_nnz() / ncol))
    while i < nrow:
        t1 = time.time()
        i2 = i + chunk_size

        # Print doubly-inclusive lo..hi like 0..17 and 18..31.
        chunk_percent = min(100, 100 * (i2 - 1) / nrow)
        logging.log_io(
            None,
            "START  chunk rows %d..%d of %d (%.3f%%)"
            % (i, i2 - 1, nrow, chunk_percent),
        )

        chunk = matrix[i:i2, :]

        if ingest_mode == "resume" and storage_ned is not None:
            chunk_bounds = matrix_bounds
            chunk_bounds[0] = (
                int(i),
                int(i2 - 1),
            )  # Cast for lint in case np.int64
            if _chunk_is_contained_in_axis(chunk_bounds, storage_ned, 0):
                # Print doubly inclusive lo..hi like 0..17 and 18..31.
                logging.log_io(
                    "... %7.3f%% done" % chunk_percent,
                    "SKIP   chunk rows %d..%d of %d (%.3f%%)"
                    % (i, i2 - 1, nrow, chunk_percent),
                )
                i = i2
                continue

        if isinstance(chunk, np.ndarray):
            tensor = pa.Tensor.from_numpy(chunk)
        else:
            tensor = pa.Tensor.from_numpy(chunk.toarray())
        soma_ndarray.write((slice(i, i2), slice(None)), tensor)

        t2 = time.time()
        chunk_seconds = t2 - t1
        eta_seconds = eta_tracker.ingest_and_predict(chunk_percent, chunk_seconds)

        if chunk_percent < 100:
            logging.log_io(
                "... %7.3f%% done, ETA %s" % (chunk_percent, eta_seconds),
                "FINISH chunk in %.3f seconds, %7.3f%% done, ETA %s"
                % (chunk_seconds, chunk_percent, eta_seconds),
            )

        i = i2

    return


def _read_nonempty_domain(arr: TileDBArray) -> Any:
    try:
        return arr._handle.reader.nonempty_domain()
    except SOMAError:
        # This means that we're open in write-only mode.
        # Reopen the array in read mode.
        pass

    cls = type(arr)
    with cls.open(arr.uri, "r", platform_config=None, context=arr.context) as readarr:
        return readarr._handle.reader.nonempty_domain()


def _find_sparse_chunk_size(
    matrix: SparseMatrix, start_index: int, axis: int, goal_chunk_nnz: int
) -> int:
    """
    Given a sparse matrix and a start index, return a step size, on the stride axis, which will
    achieve the cummulative nnz desired.

    :param matrix: The input scipy.sparse matrix.
    :param start_index: the index at which to start a chunk.
    :param axis: the stride axis, across which to find a chunk.
    :param goal_chunk_nnz: Desired number of non-zero array entries for the chunk.
    """
    chunk_size = 1
    sum_nnz = 0
    coords: List[Union[slice, int]] = [slice(None), slice(None)]

    # Empirically we find:
    # * If the input matrix is sp.csr_matrix or sp.csc_matrix then getting all these nnz values is
    #   quick.
    # * If the input matrix is anndata._core.sparse_dataset.SparseDataset -- which happens with
    #   out-of-core anndata reads -- then getting all these nnz values is prohibitively expensive.
    # * It turns out that getting a sample is quite sufficient. We do this regardless of whether
    #   the matrix is anndata._core.sparse_dataset.SparseDataset or not.
    # * The max_rows is manually defined after running experiments with 60GB .h5ad files.
    count = 0
    max_rows = 100

    for index in range(start_index, matrix.shape[axis]):
        count += 1
        coords[axis] = index
        sum_nnz += matrix[tuple(coords)].nnz
        if sum_nnz > goal_chunk_nnz:
            break
        if count > max_rows:
            break
        chunk_size += 1

    if sum_nnz > goal_chunk_nnz:
        return chunk_size

    # Solve the equation:
    #
    # sum_nnz              count
    # -------          =  -------
    # goal_chunk_nnz       result
    chunk_size = int(count * goal_chunk_nnz / sum_nnz)
    if chunk_size < 1:
        chunk_size = 1
    return chunk_size


def _write_matrix_to_sparseNDArray(
    soma_ndarray: SparseNDArray,
    matrix: Matrix,
    tiledb_create_options: TileDBCreateOptions,
    ingest_mode: IngestMode,
) -> None:
    """Write a matrix to an empty DenseNDArray"""

    def _coo_to_table(mat_coo: sp.coo_matrix, axis: int = 0, base: int = 0) -> pa.Table:
        pydict = {
            "soma_data": mat_coo.data,
            "soma_dim_0": mat_coo.row + base if base > 0 and axis == 0 else mat_coo.row,
            "soma_dim_1": mat_coo.col + base if base > 0 and axis == 1 else mat_coo.col,
        }
        return pa.Table.from_pydict(pydict)

    # There is a chunk-by-chunk already-done check for resume mode, below.
    # This full-matrix-level check here might seem redundant, but in fact it's important:
    # * By checking input bounds against storage NED here, we can see if the entire matrix
    #   was already ingested and avoid even loading chunks;
    # * By checking chunkwise we can catch the case where a matrix was already *partly*
    #   ingested.
    # * Of course, this also helps us catch already-completed writes in the non-chunked case.
    # TODO: make sure we're not using an old timestamp for this
    storage_ned = None
    if ingest_mode == "resume":
        # This lets us check for already-ingested chunks, when in resume-ingest mode.
        # THIS IS A HACK AND ONLY WORKS BECAUSE WE ARE DOING THIS BEFORE ALL WRITES.
        storage_ned = _read_nonempty_domain(soma_ndarray)
        matrix_bounds = [
            (0, int(n - 1)) for n in matrix.shape
        ]  # Cast for lint in case np.int64
        logging.log_io(
            None,
            f"Input bounds {tuple(matrix_bounds)} storage non-empty domain {storage_ned}",
        )
        if _chunk_is_contained_in(matrix_bounds, storage_ned):
            logging.log_io(
                f"Skipped {soma_ndarray.uri}", f"SKIPPED WRITING {soma_ndarray.uri}"
            )
            return

    # Write all at once?
    if not tiledb_create_options.write_X_chunked():
        soma_ndarray.write(_coo_to_table(sp.coo_matrix(matrix)))
        return

    # Or, write in chunks, striding across the most efficient slice axis

    stride_axis = 0
    if sp.isspmatrix_csc(matrix):
        # E.g. if we used anndata.X[:]
        stride_axis = 1
    if isinstance(matrix, SparseDataset) and matrix.format_str == "csc":
        # E.g. if we used anndata.X without the [:]
        stride_axis = 1

    dim_max_size = matrix.shape[stride_axis]

    eta_tracker = eta.Tracker()
    goal_chunk_nnz = tiledb_create_options.goal_chunk_nnz()

    coords = [slice(None), slice(None)]
    i = 0
    while i < dim_max_size:
        t1 = time.time()

        # Chunk size on the stride axis
        if isinstance(matrix, np.ndarray):
            chunk_size = int(math.ceil(goal_chunk_nnz / matrix.shape[stride_axis]))
        else:
            chunk_size = _find_sparse_chunk_size(matrix, i, stride_axis, goal_chunk_nnz)

        i2 = i + chunk_size

        coords[stride_axis] = slice(i, i2)
        chunk_coo = sp.coo_matrix(matrix[tuple(coords)])

        chunk_percent = min(100, 100 * (i2 - 1) / dim_max_size)

        if ingest_mode == "resume" and storage_ned is not None:
            chunk_bounds = matrix_bounds
            chunk_bounds[stride_axis] = (
                int(i),
                int(i2 - 1),
            )  # Cast for lint in case np.int64
            if _chunk_is_contained_in_axis(chunk_bounds, storage_ned, stride_axis):
                # Print doubly inclusive lo..hi like 0..17 and 18..31.
                logging.log_io(
                    "... %7.3f%% done" % chunk_percent,
                    "SKIP   chunk rows %d..%d of %d (%.3f%%), nnz=%d"
                    % (i, i2 - 1, dim_max_size, chunk_percent, chunk_coo.nnz),
                )
                i = i2
                continue

        # Print doubly inclusive lo..hi like 0..17 and 18..31.
        logging.log_io(
            None,
            "START  chunk rows %d..%d of %d (%.3f%%), nnz=%d"
            % (i, i2 - 1, dim_max_size, chunk_percent, chunk_coo.nnz),
        )

        soma_ndarray.write(_coo_to_table(chunk_coo, stride_axis, i))

        t2 = time.time()
        chunk_seconds = t2 - t1
        eta_seconds = eta_tracker.ingest_and_predict(chunk_percent, chunk_seconds)

        if chunk_percent < 100:
            logging.log_io(
                "... %7.3f%% done, ETA %s" % (chunk_percent, eta_seconds),
                "FINISH chunk in %.3f seconds, %7.3f%% done, ETA %s"
                % (chunk_seconds, chunk_percent, eta_seconds),
            )

        i = i2


def _chunk_is_contained_in(
    chunk_bounds: Sequence[Tuple[int, int]],
    storage_nonempty_domain: Optional[Sequence[Tuple[Optional[int], Optional[int]]]],
) -> bool:
    """
    Determines if a dim range is included within the array's non-empty domain.  Ranges are inclusive
    on both endpoints.  This is a helper for resume-ingest mode.

    We say "bounds" not "MBR" with the "M" for minimum: a sparse matrix might not _have_ any
    elements for some initial/final rows or columns. Suppose an input array has shape 100 x 200, so
    bounds `((0, 99), (0, 199))` -- and also suppose there are no matrix elements for column 1.
    Also suppose the matrix has already been written to TileDB-SOMA storage. The TileDB non-empty
    domain _is_ tight -- it'd say `((0, 99), (3, 197))` for example.  When we come back for a
    resume-mode ingest, we'd see the input bounds aren't contained within the storage non-empty
    domain, and erroneously declare that the data need to be rewritten.

    This is why we take the stride axis as an argument. In resume mode, it's our contract with the
    user that they declare they are retrying the exact same input file -- and we do our best to
    fulfill their ask by checking the dimension being strided on.
    """
    if storage_nonempty_domain is None:
        return False

    if len(chunk_bounds) != len(storage_nonempty_domain):
        raise SOMAError(
            f"internal error: ingest data ndim {len(chunk_bounds)} != storage ndim {len(storage_nonempty_domain)}"
        )
    for i in range(len(chunk_bounds)):
        if not _chunk_is_contained_in_axis(chunk_bounds, storage_nonempty_domain, i):
            return False
    return True


def _chunk_is_contained_in_axis(
    chunk_bounds: Sequence[Tuple[int, int]],
    storage_nonempty_domain: Sequence[Tuple[Optional[int], Optional[int]]],
    stride_axis: int,
) -> bool:
    """
    Helper function for ``_chunk_is_contained_in``.
    """
    storage_lo, storage_hi = storage_nonempty_domain[stride_axis]
    if storage_lo is None or storage_hi is None:
        # E.g. an array has had its schema created but no data written yet
        return False

    chunk_lo, chunk_hi = chunk_bounds[stride_axis]
    if chunk_lo < storage_lo or chunk_lo > storage_hi:
        return False
    if chunk_hi < storage_lo or chunk_hi > storage_hi:
        return False

    return True


# ----------------------------------------------------------------
def to_h5ad(
    experiment: Experiment,
    h5ad_path: Path,
    measurement_name: str,
    X_layer_name: str = "data",
) -> None:
    """
    Converts the experiment group to anndata format and writes it to the specified .h5ad file.

    [lifecycle: experimental]
    """
    s = util.get_start_stamp()
    logging.log_io(None, f"START  Experiment.to_h5ad -> {h5ad_path}")

    anndata = to_anndata(
        experiment, measurement_name=measurement_name, X_layer_name=X_layer_name
    )

    s2 = util.get_start_stamp()
    logging.log_io(None, f"START  write {h5ad_path}")

    anndata.write_h5ad(h5ad_path)

    logging.log_io(None, util.format_elapsed(s2, f"FINISH write {h5ad_path}"))

    logging.log_io(
        None, util.format_elapsed(s, f"FINISH Experiment.to_h5ad -> {h5ad_path}")
    )


# ----------------------------------------------------------------
def to_anndata(
    experiment: Experiment, measurement_name: str, X_layer_name: str = "data"
) -> ad.AnnData:
    """
    Converts the experiment group to anndata. Choice of matrix formats is following what we often see in input .h5ad files:

    * X as ``scipy.sparse.csr_matrix``
    * obs,var as ``pandas.dataframe``
    * obsm,varm arrays as ``numpy.ndarray``
    * obsp,varp arrays as ``scipy.sparse.csr_matrix``

    [lifecycle: experimental]
    """

    s = util.get_start_stamp()
    logging.log_io(None, "START  Experiment.to_anndata")

    measurement = experiment.ms[measurement_name]

    obs_df = experiment.obs.read().concat().to_pandas()
    obs_df.drop([SOMA_JOINID], axis=1, inplace=True)
    obs_df.set_index("obs_id", inplace=True)

    var_df = measurement.var.read().concat().to_pandas()
    var_df.drop([SOMA_JOINID], axis=1, inplace=True)
    var_df.set_index("var_id", inplace=True)

    nobs = len(obs_df.index)
    nvar = len(var_df.index)

    if X_layer_name not in measurement.X:
        raise SOMAError(
            f"X_layer_name {X_layer_name} not found in data: {measurement.X.keys()}"
        )
    X_data = measurement.X[X_layer_name]
    X_csr = None
    X_dtype = None  # some datasets have no X
    if isinstance(X_data, DenseNDArray):
        X_ndarray = X_data.read((slice(None), slice(None))).to_numpy()
        X_dtype = X_ndarray.dtype
    elif isinstance(X_data, SparseNDArray):
        X_mat = X_data.read().tables().concat().to_pandas()  # TODO: CSR/CSC options ...
        X_csr = util_scipy.csr_from_tiledb_df(X_mat, nobs, nvar)
        X_dtype = X_csr.dtype
    else:
        raise TypeError(f"Unexpected NDArray type {type(X_data)}")

    obsm = {}
    if "obsm" in measurement:
        for key in measurement.obsm.keys():
            shape = measurement.obsm[key].shape
            if len(shape) != 2:
                raise ValueError(f"expected shape == 2; got {shape}")
            matrix = measurement.obsm[key].read((slice(None),) * len(shape)).to_numpy()
            # The spelling `sp.csr_array` is more idiomatic but doesn't exist until Python 3.8
            obsm[key] = sp.csr_matrix(matrix)

    varm = {}
    if "varm" in measurement:
        for key in measurement.varm.keys():
            shape = measurement.varm[key].shape
            if len(shape) != 2:
                raise ValueError(f"expected shape == 2; got {shape}")
            matrix = measurement.varm[key].read((slice(None),) * len(shape)).to_numpy()
            # The spelling `sp.csr_array` is more idiomatic but doesn't exist until Python 3.8
            varm[key] = sp.csr_matrix(matrix)

    obsp = {}
    if "obsp" in measurement:
        for key in measurement.obsp.keys():
            matrix = measurement.obsp[key].read().tables().concat().to_pandas()
            obsp[key] = util_scipy.csr_from_tiledb_df(matrix, nobs, nobs)

    varp = {}
    if "varp" in measurement:
        for key in measurement.varp.keys():
            matrix = measurement.varp[key].read().tables().concat().to_pandas()
            varp[key] = util_scipy.csr_from_tiledb_df(matrix, nvar, nvar)

    anndata = ad.AnnData(
        X=X_csr if X_csr is not None else X_ndarray,
        obs=obs_df,
        var=var_df,
        obsm=obsm,
        varm=varm,
        obsp=obsp,
        varp=varp,
        dtype=X_dtype,
    )

    logging.log_io(None, util.format_elapsed(s, "FINISH Experiment.to_anndata"))

    return anndata
