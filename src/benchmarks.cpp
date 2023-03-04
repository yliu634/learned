#include <iostream>
#include <chrono>
#include <thread>
#include <benchmark/benchmark.h>

#include "benchmarks/tables.hpp"


using namespace _;


using RMIHash = learned_hashing::RMIHash<std::uint64_t,100>;
//BenchmarKapilLudo();
BenchmarKapilLudoFile();
//std::this_thread::sleep_for(std::chrono::seconds(5));
BenchmarKapilLinearModel(1,34,RMIHash);



BENCHMARK_MAIN();
