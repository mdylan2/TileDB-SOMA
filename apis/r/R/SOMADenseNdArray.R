#' SOMADenseNDArray
#'
#' @description
#' `SOMADenseNDArray` is a dense, N-dimensional array of `primitive` type, with
#' offset (zero-based) `int64` integer indexing on each dimension with domain
#' `[0, maxInt64)`. The `SOMADenseNDArray` has a user-defined schema, which
#' includes:
#'
#' - **type**: a `primitive` type, expressed as an Arrow type (e.g., `int64`,
#'   `float32`, etc), indicating the type of data contained within the array
#' - **shape**: the shape of the array, i.e., number and length of each
#'   dimension
#'
#' All dimensions must have a positive, non-zero length, and there must be 1 or
#' more dimensions.
#'
#' The default "fill" value for `SOMADenseNDArray` is the zero or null value of
#' the array type (e.g., Arrow.float32 defaults to 0.0).  (lifecycle: experimental)
#' @export

SOMADenseNDArray <- R6::R6Class(
  classname = "SOMADenseNDArray",
  inherit = SOMAArrayBase,

  public = list(

    #' @description Create a SOMADenseNDArray named with the URI. (lifecycle: experimental)
    #' @param type an [Arrow type][arrow::data-type] defining the type of each
    #' element in the array.
    #' @param shape a vector of integers defining the shape of the array.
    create = function(type, shape) {
      stopifnot(
        "'type' must be a valid Arrow type" =
          is_arrow_data_type(type),
        "'shape' must be a vector of positive integers" =
          is.vector(shape) && all(shape > 0)
      )

      zstd_filter_list <- tiledb::tiledb_filter_list(c(
          tiledb_zstd_filter(level = 3L)
      ))

      # create array dimensions
      # use tiledb default names like `__dim_0`
      tdb_dims <- vector(mode = "list", length = length(shape))
      for (i in seq_along(shape)) {
        tdb_dims[[i]] <- tiledb::tiledb_dim(
          name = paste0("soma_dim_", i - 1L),
          domain = bit64::as.integer64(c(0L, shape[i] - 1L)),
          tile = bit64::as.integer64(min(c(shape[i], 2048L))),
          type = "INT64"
        )
        tiledb::filter_list(tdb_dims[[i]]) <- zstd_filter_list
      }

      # create array attribute
      tdb_attr <- tiledb::tiledb_attr(
        name = "soma_data",
        type = tiledb_type_from_arrow_type(type),
        filter_list = zstd_filter_list
      )

      # array schema
      tdb_schema <- tiledb::tiledb_array_schema(
        domain = tiledb::tiledb_domain(tdb_dims),
        attrs = tdb_attr,
        sparse = FALSE,
        cell_order = "ROW_MAJOR",
        tile_order = "ROW_MAJOR",
        capacity=100000,
        offsets_filter_list = tiledb::tiledb_filter_list(c(
          tiledb::tiledb_filter("DOUBLE_DELTA"),
          tiledb::tiledb_filter("BIT_WIDTH_REDUCTION"),
          tiledb::tiledb_filter("ZSTD")
        ))
      )

      # create array
      tiledb::tiledb_array_create(uri = self$uri, schema = tdb_schema)
      private$write_object_type_metadata()
    },

    #' @description Read as an 'arrow::Table' (lifecycle: experimental)
    #' @param coords Optional `list` of integer vectors, one for each dimension, with a
    #' length equal to the number of values to read. If `NULL`, all values are
    #' read. List elements can be named when specifying a subset of dimensions.
    #' @param result_order Optional order of read results. This can be one of either
    #' `"ROW_MAJOR, `"COL_MAJOR"`, `"GLOBAL_ORDER"`, or `"UNORDERED"`.
    #' @param iterated Option boolean indicated whether data is read in call (when
    #' `FALSE`, the default value) or in several iterated steps.
    #' @param log_level Optional logging level with default value of `"warn"`.
    #' @return An [`arrow::Table`].
    read_arrow_table = function(
      coords = NULL,
      result_order = "ROW_MAJOR",
      iterated = FALSE,
      log_level = "warn"
    ) {
      uri <- self$uri

      result_order <- map_query_layout(match_query_layout(result_order))

      if (!is.null(coords)) {
          ## ensure coords is a named list, use to select dim points
          stopifnot("'coords' must be a list" = is.list(coords),
                    "'coords' must be a list of vectors or integer64" =
                        all(vapply_lgl(coords, is_vector_or_int64)),
                    "'coords' if unnamed must have length of dim names, else if named names must match dim names" =
                        (is.null(names(coords)) && length(coords) == length(self$dimnames())) ||
                        (!is.null(names(coords)) && all(names(coords) %in% self$dimnames()))
                    )

          ## if unnamed (and test for length has passed in previous statement) set names
          if (is.null(names(coords))) names(coords) <- self$dimnames()

          ## convert integer to integer64 to match dimension type
          coords <- lapply(coords, function(x) if (inherits(x, "integer")) bit64::as.integer64(x) else x)
      }

      private$dense_matrix <- FALSE

      if (isFALSE(iterated)) {
          rl <- soma_reader(uri = uri,
                            dim_points = coords,        # NULL is dealt with by soma_reader()
                            result_order = result_order,
                            loglevel = log_level)       # idem
          private$soma_reader_transform(rl)
      } else {
          ## should we error if this isn't null?
          if (!is.null(self$soma_reader_pointer)) {
              warning("pointer not null, skipping")
          } else {
              private$soma_reader_setup()
          }
          invisible(NULL)
      }
    },

    #' @description Read as a dense matrix (lifecycle: experimental)
    #' @param coords Optional `list` of integer vectors, one for each dimension, with a
    #' length equal to the number of values to read. If `NULL`, all values are
    #' read. List elements can be named when specifying a subset of dimensions.
    #' @param result_order Optional order of read results. This can be one of either
    #' `"ROW_MAJOR, `"COL_MAJOR"`, `"GLOBAL_ORDER"`, or `"UNORDERED"`.
    #' @param iterated Option boolean indicated whether data is read in call (when
    #' `FALSE`, the default value) or in several iterated steps.
    #' @param log_level Optional logging level with default value of `"warn"`.
    #' @return A `matrix` object
    read_dense_matrix = function(
      coords = NULL,
      result_order = "ROW_MAJOR",
      iterated = FALSE,
      log_level = "warn"
    ) {
      dims <- self$dimensions()
      attr <- self$attributes()
      stopifnot("Array must have two dimensions" = length(dims) == 2,
                "Array must contain columns 'soma_dim_0' and 'soma_dim_1'" =
                    all.equal(c("soma_dim_0", "soma_dim_1"), names(dims)),
                "Array must contain column 'soma_data'" = all.equal("soma_data", names(attr)))

      if (isFALSE(iterated)) {
          tbl <- self$read_arrow_table(coords = coords, result_order = result_order, log_level = log_level)
          m <- matrix(as.numeric(tbl$GetColumnByName("soma_data")),
                      nrow = length(unique(as.numeric(tbl$GetColumnByName("soma_dim_0")))),
                      ncol = length(unique(as.numeric(tbl$GetColumnByName("soma_dim_1")))),
                      byrow = result_order == "ROW_MAJOR")
      } else {
          ## should we error if this isn't null?
          if (!is.null(self$soma_reader_pointer)) {
              warning("pointer not null, skipping")
          } else {
              private$soma_reader_setup()
              private$dense_matrix <- TRUE
              private$result_order <- result_order
          }
          invisible(NULL)
      }
    },

    #' @description Write matrix data to the array. (lifecycle: experimental)
    #'
    #' @param values A `matrix`. Character dimension names are ignored because
    #' `SOMANDArray`'s use integer indexing.
    #' @param coords A `list` of integer vectors, one for each dimension, with a
    #' length equal to the number of values to write. If `NULL`, the default,
    #' the values are taken from the row and column names of `values`.
    write = function(values, coords = NULL) {
      stopifnot(
        "'values' must be a matrix" = is.matrix(values)
      )

      if (is.null(coords)) {
        coords <- list(seq_len(nrow(values)), seq_len(ncol(values)))
      }

      stopifnot(
        "'coords' must be a list of integer vectors" =
          is.list(coords) && all(vapply_lgl(coords, is.integer)),
        "length of 'coords' must match number of dimensions" =
          length(coords) == length(self$dimensions())
      )

      on.exit(private$close())
      private$open("WRITE")
      arr <- self$object
      tiledb::query_layout(arr) <- "COL_MAJOR"
      arr[] <- values
    }
  ),

  private = list(

    ## refined from base class
    soma_reader_transform = function(x) {
      tbl <- arrow::as_arrow_table(arch::from_arch_array(x, arrow::RecordBatch))
      if (isTRUE(private$dense_matrix)) {
          m <- matrix(as.numeric(tbl$GetColumnByName("soma_data")),
                      nrow = length(unique(as.numeric(tbl$GetColumnByName("soma_dim_0")))),
                      ncol = length(unique(as.numeric(tbl$GetColumnByName("soma_dim_1")))),
                      byrow = private$result_order == "ROW_MAJOR")
      } else {
          tbl
      }
    },

    ## internal state variable for dense matrix vs arrow table return
    dense_matrix = TRUE,
    result_order = "ROW_MAJOR"
  )
)
