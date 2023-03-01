#include <iostream>
#include <benchmark/benchmark.h>

#include "benchmarks/tables.hpp"


using namespace _;

using RMIHash = learned_hashing::RMIHash<std::uint64_t,100>;

BenchmarKapilLinearModel(1,34,RMIHash);



BENCHMARK_MAIN();
