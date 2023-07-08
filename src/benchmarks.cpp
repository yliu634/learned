#include <iostream>
#include <chrono>
#include <thread>
#include <benchmark/benchmark.h>

#include "benchmarks/tables.hpp"


using namespace _;

using RadixSplineHash = learned_hashing::RadixSplineHash<std::uint64_t,8,2048>;
using RMIHash = learned_hashing::RMIHash<std::uint64_t,100>;
using MWHC = exotic_hashing::MWHC<Key>;
using MURMUR = hashing::MurmurFinalizer<Key>;
using XXHash3 = hashing::XXHash3<Key>;
using AquaHash = hashing::AquaHash<Key>;
using KickingStrat = kapilmodelhashtable::KapilModelBiasedKicking<0>;

const int overalloc = 25;   //43,33,25,18,11,6;

///BenchmarKapilLinearModel(1,overalloc,RadixSplineHash);

////BenchmarKapilLudo();
////BenchmarKapilChainedModel(1,overalloc,RMIHash);
////BenchmarKapilChainedModel(1,overalloc,RadixSplineHash);
////BenchmarKapilChainedModel(4,overalloc,RadixSplineHash);
BenchmarKapilSpot(overalloc, RadixSplineHash);
////BenchmarKapilLinearModel(1,overalloc,RMIHash);
////BenchmarKapilLinearModel(1,overalloc,RadixSplineHash);

//BenchmarKapilChainedModel_RQ(1,overalloc,RadixSplineHash);
//BenchmarKapilChainedModel_RQ(4,overalloc,RadixSplineHash);
//BenchmarKapilRangeQueryMultipleLudo();
//BenchmarKapilSpot_RQ(overalloc, RadixSplineHash);
//BenchmarKapilLinearModel_RQ(4,overalloc,RMIHash);
//BenchmarKapilLinearModel_RQ(4,overalloc,RadixSplineHash);

//BenchmarKapilLudoExtInsert();
////BenchmarKapilChainedModel_IS(1,overalloc,RadixSplineHash);
////BenchmarKapilChainedModel_IS(4,overalloc,RadixSplineHash);
////BenchmarKapilSpot_IS(overalloc, RadixSplineHash);


////BenchmarKapilSpot(overalloc, RMIHash);




//BenchmarKapilLudoFile();
////BenchmarKapilLudoExtend();
//BenchmarKapilLudoExtendFile();

//BenchmarKapilLinear(1,overalloc,MURMUR);
//BenchmarKapilLinearFile(1,overalloc, MURMUR);
////BenchmarKapilLinearModel(1,overalloc,RMIHash);
////BenchmarKapilLinearModel(1,overalloc,RadixSplineHash);

//BenchmarKapilLinearModelFile(1,overalloc,RMIHash);
//BenchmarKapilLinearModelFile_RQ(1,overalloc,RMIHash);
//BenchmarKapilLinearExotic(1,overalloc,MWHC);
//BenchmarKapilLinearExoticFile(1,overalloc,MWHC);


//BenchmarKapilChained(1,overalloc,MURMUR);
//BenchmarKapilChainedFile(1,overalloc,MURMUR);


////BenchmarKapilChainedModel(1,overalloc,RadixSplineHash);
//BenchmarKapilChainedModelFile(1,overalloc,RMIHash);
//BenchmarKapilChainedModelFile_RQ(1,overalloc,RMIHash);

//BenchmarKapilCuckoo(4,200, MURMUR, KickingStrat);
//BenchmarKapilCuckooModel(4,200, RMIHash, KickingStrat);
//BenchmarKapilCuckooModelFile(4,200, RMIHash, KickingStrat);

//BenchmarKapilMultipleLudo();
//BenchmarKapilRangeQueryMultipleLudo();
//BenchmarKapilRangeQueryMultipleLudoFile();

BENCHMARK_MAIN();
