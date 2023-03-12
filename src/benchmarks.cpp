#include <iostream>
#include <chrono>
#include <thread>
#include <benchmark/benchmark.h>

#include "benchmarks/tables.hpp"


using namespace _;


using RMIHash = learned_hashing::RMIHash<std::uint64_t,100>;
using MWHC = exotic_hashing::MWHC<Key>;
using MURMUR = hashing::MurmurFinalizer<Key>;
using XXHash3 = hashing::XXHash3<Key>;
using AquaHash = hashing::AquaHash<Key>;
using MURMUR1 = hashing::MurmurFinalizer<Key>;
using KickingStrat = kapilmodelhashtable::KapilModelBiasedKicking<0>;


BenchmarKapilLudo();
BenchmarKapilLudoFile();


//BenchmarKapilLinear(1,34,MURMUR);
//BenchmarKapilLinearFile(1,34, MURMUR);
//BenchmarKapilLinearModel(1,34,RMIHash);
//BenchmarKapilLinearModelFile(1,34,RMIHash);
/*BenchmarKapilLinearExotic(1,34,MWHC);
BenchmarKapilLinearExoticFile(1,34,MWHC);

BenchmarKapilChained(1,34,MURMUR);
BenchmarKapilChainedFile(1,10050,MURMUR);
BenchmarKapilChainedModel(1,34,RMIHash);

BenchmarKapilChainedModelFile(1,34,RMIHash);
BenchmarKapilChainedExotic(1,34,MWHC);
BenchmarKapilChainedExoticFile(1,34,MWHC);
*/

//BenchmarKapilCuckoo(1,34, MURMUR1, KickingStrat);



BENCHMARK_MAIN();
