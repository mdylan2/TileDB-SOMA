from dataclasses import dataclass, field, InitVar
from typing import List, Dict, Optional, Union

import tiledb


def default_X_data_row_filters() -> List[tiledb.Filter]:
    return [
        tiledb.RleFilter(),
    ]


def default_X_data_col_filters() -> List[tiledb.Filter]:
    return [
        tiledb.ZstdFilter(3),
    ]


def default_X_data_offset_filters() -> List[tiledb.Filter]:
    return [
        tiledb.DoubleDeltaFilter(),
        tiledb.BitWidthReductionFilter(),
        tiledb.ZstdFilter(),
    ]


def default_X_data_attr_filters() -> List[tiledb.Filter]:
    return [
        tiledb.ZstdFilter(),
    ]


# https://docs.python.org/3/library/dataclasses.html
# ValueError: mutable default <class 'list'> for field X_data_row_filters is not allowed: use default_factory
@dataclass(unsafe_hash=True)
class SOMAOptions:
    """
    A place to put configuration options various users may wish to change.
    These are mainly TileDB array-schema parameters.
    """

    # TODO: pending further work on
    # https://github.com/single-cell-data/TileDB-SingleCell/issues/27
    obs_extent: int = 256
    var_extent: int = 2048

    X_data_row_filters: List[tiledb.Filter] = field(default_factory=default_X_data_row_filters)
    X_data_col_filters: List[tiledb.Filter] = field(default_factory=default_X_data_col_filters)
    X_data_offset_filters: List[tiledb.Filter] = field(default_factory=default_X_data_offset_filters)
    X_data_attr_filters: List[tiledb.Filter] = field(default_factory=default_X_data_attr_filters)

    array_types: InitVar[Dict[str, Dict[str, Union[int, str]]]] = {
        "row_opt": {"capacity": 1000, "tile_order": "row-major", "cell_order": "row-major"},
        "col_opt": {"capacity": 1000, "tile_order": "col-major", "cell_order": "col-major"},
        "hybrid_1": {"capacity": 1000, "tile_order": "row-major", "cell_order": "col-major"},
        "hybrid_2": {"capacity": 1000, "tile_order": "col-major", "cell_order": "row-major"},
    }

    # Appropriate for SOMA use-cases: typically X queries are _not_ anything like subarray queries,
    # but rather, "pepper queries" with many obs_ids and/or var_ids sprinkled throughout the
    # dimension space. Analysis reveals that smaller capacities are helpful here in order to
    # increase the ratio of used-cells-per-tile to all-cells-per-tile.
    X_capacity: int = 1000
    X_tile_order: str = "row-major"
    X_cell_order: str = "row-major"

    df_capacity: int = 10000
    df_tile_order: str = "row-major"
    df_cell_order: str = "row-major"

    # https://github.com/single-cell-data/TileDB-SingleCell/issues/27
    string_dim_zstd_level: int = 3

    write_X_chunked: bool = True
    goal_chunk_nnz: int = 20_000_000
    # Allows relocatability for local disk / S3, and correct behavior for TileDB Cloud
    member_uris_are_relative: Optional[bool] = None

    max_thread_pool_workers: int = 8

    # This part assumes only default, but technically, we could switch it around to take any kwargs and then apply the defaults
    def __post_init__(self, array_types):
        for array_type, dict_ in array_types.items():
            tile_order = dict_.get("tile_order", self.X_tile_order)
            cell_order = dict_.get("cell_order", self.X_cell_order)
            capacity = dict_.get("capacity", self.X_capacity)

            setattr(self, f"X__{array_type}__tile_order", tile_order)
            setattr(self, f"X__{array_type}__cell_order", cell_order)
            setattr(self, f"X__{array_type}__capacity", capacity)
