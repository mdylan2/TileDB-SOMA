test_that("Basic mechanics", {

  # TODO: Determine why this fails only on linux w/ rcmdcheck::rcmdcheck()
  testthat::skip_on_covr()

  ## This variable appears to be set under rcmdcheck (hence also with covr), but
  ## not with R CMD CHECK. As the testscript appears to fail here for rcmdcheck
  ## only (why?), we bail. Sadly this does not seem to help with the covr fail.
  testthat::skip_if(Sys.getenv("CALLR_IS_RUNNING", "") != "")

  uri <- withr::local_tempdir("soma-ms")

  measurement <- SOMAMeasurement$new(uri)

  expect_false(measurement$exists())
  expect_error(measurement$var, "Group does not exist.")

  measurement$create()
  # TODO: Determine behavior for retrieving empty obs/ms
  # expect_null(experiment$var)
  # expect_null(experiment$X)

  # Add var
  expect_error(measurement$var, "No member named 'var' found")

  var <- create_and_populate_obs(file.path(uri, "var"))

  measurement$var <- var
  expect_equal(measurement$length(), 1)
  expect_true(inherits(measurement$var, "SOMADataFrame"))

  # Add X collection
  expect_error(measurement$X, "No member named 'X' found")
  expect_error(measurement$X <- var, "X must be a 'SOMACollection'")

  X <- SOMACollection$new(file.path(uri, "X"))
  X$create()

  measurement$X <- X
  expect_true(inherits(measurement$X, "SOMACollection"))
  expect_equal(measurement$length(), 2)

  # Add X layer
  expect_equal(measurement$X$length(), 0)
  nda <- create_and_populate_sparse_nd_array(file.path(uri, "X", "RNA"))
  measurement$X$set(nda, name = "RNA")
  expect_equal(measurement$X$length(), 1)
})
