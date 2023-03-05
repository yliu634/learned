#include <iostream>
#include <chrono>
#include <thread>
#include <benchmark/benchmark.h>

#include "benchmarks/tables.hpp"


using namespace _;


using RMIHash = learned_hashing::RMIHash<std::uint64_t,100>;
using MWHC = exotic_hashing::MWHC<Key>;
BenchmarKapilLudo();
BenchmarKapilLudoFile();
BenchmarKapilLinearModel(1,34,RMIHash);
BenchmarKapilLinearModelFile(1,34,RMIHash);
//BenchmarKapilLinearExotic(1,34,MWHC);
//BenchmarKapilLinearExoticFile(1,34,MWHC);


BENCHMARK_MAIN();
