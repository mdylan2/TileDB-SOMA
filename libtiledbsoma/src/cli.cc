/**
 * @file   cli.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file is currently a sandbox for C++ API experiments
 */

#include <tiledbsoma/tiledbsoma>

using namespace tiledbsoma;

void test_sdf(const std::string& uri) {
    std::map<std::string, std::string> config;
    // config["soma.init_buffer_bytes"] = "4294967296";
    // config["sm.mem.total_budget"] = "1118388608";

    auto obs = SOMAReader::open(uri + "/obs", "obs");
    auto var = SOMAReader::open(uri + "/var", "var");
    auto obs_data = obs->read_next();
    auto var_data = var->read_next();
    if (obs->results_complete() && var->results_complete()) {
        LOG_INFO("var and obs queries are complete");
    }

    auto x_data = SOMAReader::open(uri + "/X/data", "X/data", config);
    int batches = 0;
    int total_num_rows = 0;
    while (auto batch = x_data->read_next()) {
        batches++;
        total_num_rows += batch.value()->at("obs_id")->size();
    }
    LOG_INFO(fmt::format("X/data rows = {}", total_num_rows));
    LOG_INFO(fmt::format("  batches = {}", batches));
}

int main(int argc, char** argv) {
    LOG_CONFIG("debug");

    (void)argc;
    (void)argv;

    try {
        test_sdf(argv[1]);
    } catch (const std::exception& e) {
        printf("%s\n", e.what());
        return 1;
    }

    return 0;
};
