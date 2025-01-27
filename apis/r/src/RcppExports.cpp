// Generated by using Rcpp::compileAttributes() -> do not edit by hand
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include "../inst/include/tiledbsoma_types.h"
#include <Rcpp.h>

using namespace Rcpp;

#ifdef RCPP_USE_GLOBAL_ROSTREAM
Rcpp::Rostream<true>&  Rcpp::Rcout = Rcpp::Rcpp_cout_get();
Rcpp::Rostream<false>& Rcpp::Rcerr = Rcpp::Rcpp_cerr_get();
#endif

// soma_reader
Rcpp::List soma_reader(const std::string& uri, Rcpp::Nullable<Rcpp::CharacterVector> colnames, Rcpp::Nullable<Rcpp::XPtr<tiledb::QueryCondition>> qc, Rcpp::Nullable<Rcpp::List> dim_points, Rcpp::Nullable<Rcpp::List> dim_ranges, std::string batch_size, std::string result_order, const std::string& loglevel);
RcppExport SEXP _tiledbsoma_soma_reader(SEXP uriSEXP, SEXP colnamesSEXP, SEXP qcSEXP, SEXP dim_pointsSEXP, SEXP dim_rangesSEXP, SEXP batch_sizeSEXP, SEXP result_orderSEXP, SEXP loglevelSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::string& >::type uri(uriSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::CharacterVector> >::type colnames(colnamesSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::XPtr<tiledb::QueryCondition>> >::type qc(qcSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::List> >::type dim_points(dim_pointsSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::List> >::type dim_ranges(dim_rangesSEXP);
    Rcpp::traits::input_parameter< std::string >::type batch_size(batch_sizeSEXP);
    Rcpp::traits::input_parameter< std::string >::type result_order(result_orderSEXP);
    Rcpp::traits::input_parameter< const std::string& >::type loglevel(loglevelSEXP);
    rcpp_result_gen = Rcpp::wrap(soma_reader(uri, colnames, qc, dim_points, dim_ranges, batch_size, result_order, loglevel));
    return rcpp_result_gen;
END_RCPP
}
// set_log_level
void set_log_level(const std::string& level);
RcppExport SEXP _tiledbsoma_set_log_level(SEXP levelSEXP) {
BEGIN_RCPP
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::string& >::type level(levelSEXP);
    set_log_level(level);
    return R_NilValue;
END_RCPP
}
// get_column_types
Rcpp::CharacterVector get_column_types(const std::string& uri, const std::vector<std::string>& colnames);
RcppExport SEXP _tiledbsoma_get_column_types(SEXP uriSEXP, SEXP colnamesSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::string& >::type uri(uriSEXP);
    Rcpp::traits::input_parameter< const std::vector<std::string>& >::type colnames(colnamesSEXP);
    rcpp_result_gen = Rcpp::wrap(get_column_types(uri, colnames));
    return rcpp_result_gen;
END_RCPP
}
// nnz
double nnz(const std::string& uri);
RcppExport SEXP _tiledbsoma_nnz(SEXP uriSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< const std::string& >::type uri(uriSEXP);
    rcpp_result_gen = Rcpp::wrap(nnz(uri));
    return rcpp_result_gen;
END_RCPP
}
// sr_setup
Rcpp::XPtr<tdbs::SOMAReader> sr_setup(Rcpp::XPtr<tiledb::Context> ctx, const std::string& uri, Rcpp::Nullable<Rcpp::CharacterVector> colnames, Rcpp::Nullable<Rcpp::XPtr<tiledb::QueryCondition>> qc, Rcpp::Nullable<Rcpp::List> dim_points, Rcpp::Nullable<Rcpp::List> dim_ranges, Rcpp::Nullable<Rcpp::CharacterVector> config, const std::string& loglevel);
RcppExport SEXP _tiledbsoma_sr_setup(SEXP ctxSEXP, SEXP uriSEXP, SEXP colnamesSEXP, SEXP qcSEXP, SEXP dim_pointsSEXP, SEXP dim_rangesSEXP, SEXP configSEXP, SEXP loglevelSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< Rcpp::XPtr<tiledb::Context> >::type ctx(ctxSEXP);
    Rcpp::traits::input_parameter< const std::string& >::type uri(uriSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::CharacterVector> >::type colnames(colnamesSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::XPtr<tiledb::QueryCondition>> >::type qc(qcSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::List> >::type dim_points(dim_pointsSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::List> >::type dim_ranges(dim_rangesSEXP);
    Rcpp::traits::input_parameter< Rcpp::Nullable<Rcpp::CharacterVector> >::type config(configSEXP);
    Rcpp::traits::input_parameter< const std::string& >::type loglevel(loglevelSEXP);
    rcpp_result_gen = Rcpp::wrap(sr_setup(ctx, uri, colnames, qc, dim_points, dim_ranges, config, loglevel));
    return rcpp_result_gen;
END_RCPP
}
// sr_complete
bool sr_complete(Rcpp::XPtr<tdbs::SOMAReader> sr);
RcppExport SEXP _tiledbsoma_sr_complete(SEXP srSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< Rcpp::XPtr<tdbs::SOMAReader> >::type sr(srSEXP);
    rcpp_result_gen = Rcpp::wrap(sr_complete(sr));
    return rcpp_result_gen;
END_RCPP
}
// sr_next
Rcpp::List sr_next(Rcpp::XPtr<tdbs::SOMAReader> sr);
RcppExport SEXP _tiledbsoma_sr_next(SEXP srSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< Rcpp::XPtr<tdbs::SOMAReader> >::type sr(srSEXP);
    rcpp_result_gen = Rcpp::wrap(sr_next(sr));
    return rcpp_result_gen;
END_RCPP
}
// tiledbsoma_stats_enable
void tiledbsoma_stats_enable();
RcppExport SEXP _tiledbsoma_stats_enable() {
BEGIN_RCPP
    Rcpp::RNGScope rcpp_rngScope_gen;
    tiledbsoma_stats_enable();
    return R_NilValue;
END_RCPP
}
// tiledbsoma_stats_disable
void tiledbsoma_stats_disable();
RcppExport SEXP _tiledbsoma_stats_disable() {
BEGIN_RCPP
    Rcpp::RNGScope rcpp_rngScope_gen;
    tiledbsoma_stats_disable();
    return R_NilValue;
END_RCPP
}
// tiledbsoma_stats_reset
void tiledbsoma_stats_reset();
RcppExport SEXP _tiledbsoma_stats_reset() {
BEGIN_RCPP
    Rcpp::RNGScope rcpp_rngScope_gen;
    tiledbsoma_stats_reset();
    return R_NilValue;
END_RCPP
}
// tiledbsoma_stats_dump
std::string tiledbsoma_stats_dump();
RcppExport SEXP _tiledbsoma_stats_dump() {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    rcpp_result_gen = Rcpp::wrap(tiledbsoma_stats_dump());
    return rcpp_result_gen;
END_RCPP
}

static const R_CallMethodDef CallEntries[] = {
    {"_tiledbsoma_soma_reader", (DL_FUNC) &_tiledbsoma_soma_reader, 8},
    {"_tiledbsoma_set_log_level", (DL_FUNC) &_tiledbsoma_set_log_level, 1},
    {"_tiledbsoma_get_column_types", (DL_FUNC) &_tiledbsoma_get_column_types, 2},
    {"_tiledbsoma_nnz", (DL_FUNC) &_tiledbsoma_nnz, 1},
    {"_tiledbsoma_sr_setup", (DL_FUNC) &_tiledbsoma_sr_setup, 8},
    {"_tiledbsoma_sr_complete", (DL_FUNC) &_tiledbsoma_sr_complete, 1},
    {"_tiledbsoma_sr_next", (DL_FUNC) &_tiledbsoma_sr_next, 1},
    {"_tiledbsoma_stats_enable", (DL_FUNC) &_tiledbsoma_stats_enable, 0},
    {"_tiledbsoma_stats_disable", (DL_FUNC) &_tiledbsoma_stats_disable, 0},
    {"_tiledbsoma_stats_reset", (DL_FUNC) &_tiledbsoma_stats_reset, 0},
    {"_tiledbsoma_stats_dump", (DL_FUNC) &_tiledbsoma_stats_dump, 0},
    {NULL, NULL, 0}
};

RcppExport void R_init_tiledbsoma(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
