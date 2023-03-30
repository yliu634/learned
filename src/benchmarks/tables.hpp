#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iostream>
#include <iterator>
#include <functional>
#include <limits>
#include <ostream>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <string>

#include <hashing.hpp>
#include <hashtable.hpp>
#include <masters_thesis.hpp>
#include <learned_hashing.hpp>

#include <stdio.h>
#include <stdlib.h>

#include "../../include/blob/filestore.hpp"
#include "../../thirdparty/perfevent/PerfEvent.hpp"
#include "../support/datasets.hpp"
#include "../support/probing_set.hpp"

#include "include/convenience/builtins.hpp"
#include "include/mmphf/rank_hash.hpp"
#include "include/rmi.hpp"

namespace _ {

using Key = std::uint64_t;
using Payload = std::uint64_t;

const std::vector<std::int64_t> probe_distributions{
    // static_cast<std::underlying_type_t<dataset::ProbingDistribution>>(
    //     dataset::ProbingDistribution::EXPONENTIAL_SORTED),
    // static_cast<std::underlying_type_t<dataset::ProbingDistribution>>(
    //     dataset::ProbingDistribution::EXPONENTIAL_RANDOM),
    static_cast<std::underlying_type_t<dataset::ProbingDistribution>>(
        dataset::ProbingDistribution::UNIFORM)};

const std::vector<std::int64_t> dataset_sizes{1000000}; // {100000000}
const std::vector<std::int64_t> succ_probability{100};
const std::vector<std::int64_t> point_query_prop{0,10,20,30,40,50,60,70,80,90,100};
const std::vector<std::int64_t> datasets {
    static_cast<std::underlying_type_t<dataset::ID>>(dataset::ID::WIKI),
    //static_cast<std::underlying_type_t<dataset::ID>>(dataset::ID::GAPPED_10),
    //static_cast<std::underlying_type_t<dataset::ID>>(dataset::ID::UNIFORM),
    //static_cast<std::underlying_type_t<dataset::ID>>(dataset::ID::NORMAL),
    //static_cast<std::underlying_type_t<dataset::ID>>(dataset::ID::SEQUENTIAL)
    // static_cast<std::underlying_type_t<dataset::ID>>(dataset::ID::OSM),
    // static_cast<std::underlying_type_t<dataset::ID>>(dataset::ID::FB)
  };

template <class Table>
static void Construction(benchmark::State& state) {
  std::random_device rd;
  std::default_random_engine rng(rd());

  // Extract variables
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));

  // Generate data (keys & payloads) & probing set
  std::vector<std::pair<Key, Payload>> data;
  data.reserve(dataset_size);
  {
    auto keys = dataset::load_cached<Key>(did, dataset_size);

    std::transform(keys.begin(), keys.end(), std::back_inserter(data),
                   [](const Key& key) { return std::make_pair(key, key - 5); });
  }

  if (data.empty()) {
    // otherwise google benchmark produces an error ;(
    for (auto _ : state) {
    }
    return;
  }

  // build table
  size_t total_bytes, directory_bytes;
  std::string name;
  for (auto _ : state) {
    Table table(data);

    total_bytes = table.byte_size();
    directory_bytes = table.directory_byte_size();
    name = table.name();
  }

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = total_bytes;
  state.counters["table_directory_bytes"] = directory_bytes;
  state.counters["table_model_bytes"] = total_bytes - directory_bytes;
  state.counters["table_bits_per_key"] = 8. * total_bytes / data.size();
  state.counters["data_elem_count"] = data.size();
  state.SetLabel(name + ":" + dataset::name(did));
}

std::string previous_signature = "";
std::vector<Key> probing_set{};
void* prev_table = nullptr;
std::function<void()> free_lambda = []() {};

template <class Table, size_t RangeSize>
static void TableProbe(benchmark::State& state) {

  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));

  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(RangeSize) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);
  if (previous_signature != signature) {
    std::cout << "performing setup... .";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    std::vector<std::pair<Key, Payload>> data{};
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);

      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      int succ_probability=100;
      probing_set = dataset::generate_probing_set(keys,probing_dist,succ_probability);
    }

    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      std::cout << "failed" << std::endl;
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;
  }
  previous_signature = signature;

  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;

  size_t i = 0;
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const auto searched = probing_set[i++];

    // Lower bound lookup
    auto it = table->operator[](
        searched);  // TODO: does this generate a 'call' op? =>
                    // https://stackoverflow.com/questions/10631283/how-will-i-know-whether-inline-function-is-actually-replaced-at-the-place-where

    // RangeSize == 0 -> only key lookup
    // RangeSize == 1 -> key + payload lookup
    // RangeSize  > 1 -> lb key + multiple payload lookups
    if constexpr (RangeSize == 0) {
      benchmark::DoNotOptimize(it);
    } else if constexpr (RangeSize == 1) {
      const auto payload = it.payload();
      benchmark::DoNotOptimize(payload);
    } else if constexpr (RangeSize > 1) {
      Payload total = 0;
      for (size_t i = 0; it != table->end() && i < RangeSize; i++, ++it) {
        total ^= it.payload();
      }
      benchmark::DoNotOptimize(total);
    }
    full_mem_barrier;
  }

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist));
}

// mix of point lookup and range query
template <class Table>
static void TableMixedLookup(benchmark::State& state) {
  std::random_device rd;
  std::default_random_engine rng(rd());

  // Extract variables
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
  const auto percentage_of_point_queries = static_cast<size_t>(state.range(3));

  std::string signature = std::string(typeid(Table).name()) + "_" +
                          std::to_string(percentage_of_point_queries) + "_" +
                          std::to_string(dataset_size) + "_" +
                          dataset::name(did) + "_" +
                          dataset::name(probing_dist);
  if (previous_signature != signature) {
    std::cout << "performing setup... ..";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    std::vector<std::pair<Key, Payload>> data{};
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);

      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      int succ_probability=100;
      probing_set = dataset::generate_probing_set(keys, probing_dist,succ_probability);
    }

    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;
  }
  previous_signature = signature;

  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;

  // distribution
  std::uniform_int_distribution<size_t> point_query_dist(1, 100);

  size_t i = 0;
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const auto searched = probing_set[i++];

    // Lower bound lookup
    auto it = table->operator[](searched);
    if (it != table->end()) {
      const auto lb_payload = it.payload();
      benchmark::DoNotOptimize(lb_payload);

      // Chance based perform full range scan
      if (point_query_dist(rng) > percentage_of_point_queries) {
        ++it;
        Payload total = 0;
        for (size_t i = 1; it != table->end() && i < 10; i++, ++it) {
          total ^= it.payload();
        }
        benchmark::DoNotOptimize(total);
      }
    }

    full_mem_barrier;
  }

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["point_lookup_percent"] =
      static_cast<double>(percentage_of_point_queries) / 100.0;
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist));
}







template <class Table, size_t RangeSize>
static void PointProbe(benchmark::State& state) {
  // Extract variables
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
   const auto succ_probability =
      static_cast<size_t>(state.range(3)); 
  

  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(RangeSize) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);

  if(previous_signature!=signature) {
    std::cout<<"Probing set size is: "<<probing_set.size()<<std::endl;
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
  }
     
  if (previous_signature != signature) {
    std::cout << "performing setup... ";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    std::vector<std::pair<Key, Payload>> data{};
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);

      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      // int succ_probability=100;
      probing_set = dataset::generate_probing_set(keys, probing_dist,succ_probability);
    }

    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      std::cout << "failed" << std::endl;
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;
    
    std::sort(data.begin(), data.end(),[](const auto& a, const auto& b) { return a.first < b.first; });
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
    // table->print_data_statistics();

    Table* table = (Table*)prev_table;

    table->print_data_statistics();

    uint64_t total_sum=0;
    uint64_t query_count=100000;

  }
  
  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;
  previous_signature = signature;  

  size_t i = 0;
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const auto searched = probing_set[i%probing_set.size()];
    i++;

    // Lower bound lookup
    // auto it = table->useless_func();
    auto it = table->operator[](searched);  // TODO: does this generate a 'call' op? =>
                    // https://stackoverflow.com/questions/10631283/how-will-i-know-whether-inline-function-is-actually-replaced-at-the-place-where
    //int it = table->lookUp(searched, value);
    benchmark::DoNotOptimize(it);
    // __sync_synchronize();
    // full_mem_barrier;
  }
  table->print_data_statistics(true);

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;

  std::stringstream ss;
  ss << succ_probability;
  std::string temp = ss.str();
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist)+":"+temp);
}


template <class Table, size_t RangeSize, uint32_t blockSize = 512>
static void PointProbeFile(benchmark::State& state) {
  
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
   const auto succ_probability =
      static_cast<size_t>(state.range(3)); 
         
  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(RangeSize) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);

  if(previous_signature!=signature) {
    std::cout<<"Probing set size is: "<<probing_set.size()<<std::endl;
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
  }
     
  if (previous_signature != signature) {
    std::cout << "performing setup... ";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    std::vector<std::pair<Key, Payload>> data{};
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);

      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      // int succ_probability=100;
      probing_set = dataset::generate_probing_set(keys, probing_dist,succ_probability);
    }

    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      std::cout << "failed" << std::endl;
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;
    
    std::sort(data.begin(), data.end(),[](const auto& a, const auto& b) { return a.first < b.first; });
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
    
    Table* table = (Table*)prev_table;

    table->print_data_statistics();

    uint64_t total_sum=0;
    uint64_t query_count=100000;
  }
  
  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;
  previous_signature = signature;  

  size_t i = 0;
  std::cout << "start into loop." << std::endl;
  std::vector<char> value(blockSize);
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const auto searched = probing_set[i%probing_set.size()];
    i++;

    int it = table->lookUp(searched, value);
    benchmark::DoNotOptimize(it);
  }
  table->print_data_statistics(true);     // Yi


  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;

  std::stringstream ss;
  ss << succ_probability;
  std::string temp = ss.str();
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist)+":"+temp);

}


template <class Table, size_t RangeSize>
static void CollisionStats(benchmark::State& state) {
  // Extract variables
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
   const auto succ_probability =
      static_cast<size_t>(state.range(3));    

  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(RangeSize) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);
  if (previous_signature != signature) {
    std::cout << "performing setup... ... .";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    std::vector<std::pair<Key, Payload>> data{};
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);

      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      // int succ_probability=100;
      probing_set = dataset::generate_probing_set(keys, probing_dist,succ_probability);
    }

    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      std::cout << "failed" << std::endl;
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;
    

  }
  

  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;

  if (previous_signature != signature)
  {
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
    table->print_data_statistics();
  }

  // std::cout<<"signature swap"<<std::endl;

  previous_signature = signature;  

  // std::cout<<"again?"<<std::endl;

  size_t i = 0;
  for (auto _ : state) {
    // while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    // const auto searched = probing_set[i%probing_set.size()];
    // i++;

    // Lower bound lookup
    auto it = table->useless_func();  // TODO: does this generate a 'call' op? =>
                    // https://stackoverflow.com/questions/10631283/how-will-i-know-whether-inline-function-is-actually-replaced-at-the-place-where

    benchmark::DoNotOptimize(it);
    __sync_synchronize();
    // full_mem_barrier;
  }

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;

  std::stringstream ss;
  ss << succ_probability;
  std::string temp = ss.str();
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist)+":"+temp);
}






// ########################### START BECHMARKING ##########################

using namespace masters_thesis;


#define BM(Table)                                                              \
  BENCHMARK_TEMPLATE(Construction, Table)                                      \
      ->ArgsProduct({dataset_sizes, datasets});                                \
  BENCHMARK_TEMPLATE(TableMixedLookup, Table)                                  \
      ->ArgsProduct(                                                           \
          {{100000000}, datasets, probe_distributions, {0, 25, 50, 75, 100}}); \
  BENCHMARK_TEMPLATE(TableProbe, Table, 0)                                     \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions});           \
  BENCHMARK_TEMPLATE(TableProbe, Table, 1)                                     \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions});           \
  BENCHMARK_TEMPLATE(TableProbe, Table, 10)                                    \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions});           \
  BENCHMARK_TEMPLATE(TableProbe, Table, 20)                                    \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions});


#define BenchmarkMonotone(BucketSize, Model)                    \
  using MonotoneHashtable##BucketSize##Model =                  \
      MonotoneHashtable<Key, Payload, BucketSize, Model>;       \
  BM(MonotoneHashtable##BucketSize##Model);                     \
  using PrefetchedMonotoneHashtable##BucketSize##Model =        \
      MonotoneHashtable<Key, Payload, BucketSize, Model, true>; \
  BM(PrefetchedMonotoneHashtable##BucketSize##Model);

#define BenchmarkMMPHFTable(MMPHF)                           \
  using MMPHFTable##MMPHF = MMPHFTable<Key, Payload, MMPHF>; \
  BM(MMPHFTable##MMPHF);


#define KAPILBM(Table)                                                              \
  BENCHMARK_TEMPLATE(PointProbe, Table, 0)                                     \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions,succ_probability}) \
      ->Iterations(200000);

#define KAPILBMFILE(Table)                                                              \
  BENCHMARK_TEMPLATE(PointProbeFile, Table, 0, 512)                                     \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions,succ_probability}) \
      ->Iterations(200000);









// ############################## LINEAR PROBING ##############################
// ############################## LINEAR PROBING ##############################
// ############################## LINEAR PROBING ##############################

#define BenchmarKapilLinear(BucketSize,OverAlloc,HashFn)                           \
  using KapilLinearHashTable##BucketSize##OverAlloc##HashFn = KapilLinearHashTable<Key, Payload, BucketSize,OverAlloc, HashFn>; \
  KAPILBM(KapilLinearHashTable##BucketSize##OverAlloc##HashFn);

#define BenchmarKapilLinearFile(BucketSize,OverAlloc,HashFn)                           \
  using KapilLinearHashTableFile##BucketSize##OverAlloc##HashFn = KapilLinearHashTableFile<Key, Payload, BucketSize,OverAlloc, HashFn>; \
  KAPILBMFILE(KapilLinearHashTableFile##BucketSize##OverAlloc##HashFn);

#define BenchmarKapilLinearModel(BucketSize,OverAlloc,Model)                           \
  using KapilLinearModelHashTable##BucketSize##OverAlloc##Model = KapilLinearModelHashTable<Key, Payload, BucketSize,OverAlloc, Model>; \
  KAPILBM(KapilLinearModelHashTable##BucketSize##OverAlloc##Model);

#define BenchmarKapilLinearModelFile(BucketSize,OverAlloc,Model)                           \
  using KapilLinearModelHashTableFile##BucketSize##OverAlloc##Model = KapilLinearModelHashTableFile<Key, Payload, BucketSize,OverAlloc, Model>; \
  KAPILBMFILE(KapilLinearModelHashTableFile##BucketSize##OverAlloc##Model);

#define BenchmarKapilLinearExotic(BucketSize,OverAlloc,MMPHF)                           \
  using KapilLinearExoticHashTable##BucketSize##MMPHF = KapilLinearExoticHashTable<Key, Payload, BucketSize,OverAlloc, MMPHF>; \
  KAPILBM(KapilLinearExoticHashTable##BucketSize##MMPHF);

#define BenchmarKapilLinearExoticFile(BucketSize,OverAlloc,MMPHF)                           \
  using KapilLinearExoticHashTableFile##BucketSize##MMPHF = KapilLinearExoticHashTableFile<Key, Payload, BucketSize,OverAlloc, MMPHF>; \
  KAPILBMFILE(KapilLinearExoticHashTableFile##BucketSize##MMPHF);
// ############################ END LINEAR PROBING ######################







// ############################## Chaining ##############################
// ############################## Chaining ##############################
// ############################## Chaining ##############################

#define BenchmarKapilChained(BucketSize,OverAlloc,HashFn)                           \
  using KapilChainedHashTable##BucketSize##OverAlloc##HashFn = KapilChainedHashTable<Key, Payload, BucketSize,OverAlloc, HashFn>; \
  KAPILBM(KapilChainedHashTable##BucketSize##OverAlloc##HashFn);

#define BenchmarKapilChainedFile(BucketSize,OverAlloc,HashFn)                           \
  using KapilChainedHashTableFile##BucketSize##OverAlloc##HashFn = KapilChainedHashTableFile<Key, Payload, BucketSize,OverAlloc, HashFn>; \
  KAPILBMFILE(KapilChainedHashTableFile##BucketSize##OverAlloc##HashFn);

#define BenchmarKapilChainedExotic(BucketSize,OverAlloc,MMPHF)                           \
  using KapilChainedExoticHashTable##BucketSize##MMPHF = KapilChainedExoticHashTable<Key, Payload, BucketSize,OverAlloc, MMPHF>; \
  KAPILBM(KapilChainedExoticHashTable##BucketSize##MMPHF);

#define BenchmarKapilChainedExoticFile(BucketSize,OverAlloc,MMPHF)                           \
  using KapilChainedExoticHashTableFile##BucketSize##MMPHF = KapilChainedExoticHashTableFile<Key, Payload, BucketSize,OverAlloc, MMPHF>; \
  KAPILBMFILE(KapilChainedExoticHashTableFile##BucketSize##MMPHF);

#define BenchmarKapilChainedModel(BucketSize,OverAlloc,Model)                             \
  using KapilChainedModelHashTable##BucketSize##OverAlloc##Model = KapilChainedModelHashTable<Key, Payload, BucketSize,OverAlloc, Model>; \
  KAPILBM(KapilChainedModelHashTable##BucketSize##OverAlloc##Model);

#define BenchmarKapilChainedModelFile(BucketSize,OverAlloc,Model)                          \
  using KapilChainedModelHashTableFile##BucketSize##OverAlloc##Model = KapilChainedModelHashTableFile<Key, Payload, BucketSize,OverAlloc, Model>; \
  KAPILBMFILE(KapilChainedModelHashTableFile##BucketSize##OverAlloc##Model);

// ############################## END Chaining #####################################
//const std::vector<std::int64_t> bucket_size_chain{1,2,4,8};
//const std::vector<std::int64_t> overalloc_chain{10,25,50,100};








// ############################## CUCKOO HASHING ##############################
// ############################## CUCKOO HASHING ##############################
// ############################## CUCKOO HASHING ##############################

template <class Table, size_t RangeSize>
static void PointProbeCuckoo(benchmark::State& state) {
  // Extract variables
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
  const auto succ_probability =
      static_cast<size_t>(state.range(3));

  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(RangeSize) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);
  std::vector<std::pair<Key, Payload>> data{};

  if (previous_signature != signature) {
    std::cout << "performing setup... ... ..";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);
      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      probing_set = dataset::generate_probing_set(keys, probing_dist,succ_probability);
    }

    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      std::cout << "data is empty: failed" << std::endl;
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;

  }
  
  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;

  if (previous_signature != signature) {
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
    table->print_data_statistics();
  }

  previous_signature = signature;  

  size_t i = 0;
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const auto searched = probing_set[i++];

    // Lower bound lookup
    auto it = table->lookup(searched);  // TODO: does this generate a 'call' op? =>

    benchmark::DoNotOptimize(it);

  }

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;

  std::stringstream ss;
  ss << succ_probability;
  std::string temp = ss.str();
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist)+":"+temp);

}

#define KAPILBMCuckoo(Table)                                                        \
  BENCHMARK_TEMPLATE(PointProbeCuckoo, Table, 0)                                     \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions,succ_probability}) \
      ->Iterations(200000);

#define BenchmarKapilCuckoo(BucketSize,OverAlloc,HashFn,KickingStrat)                           \
  using MURMUR1 = hashing::MurmurFinalizer<Key>; \
  using KapilCuckooHashTable##BucketSize##OverAlloc##HashFn##KickingStrat = kapilhashtable::KapilCuckooHashTable<Key, Payload, BucketSize,OverAlloc, HashFn, MURMUR1,KickingStrat>; \
  KAPILBMCuckoo(KapilCuckooHashTable##BucketSize##OverAlloc##HashFn##KickingStrat);

#define BenchmarKapilCuckooFile(BucketSize,OverAlloc,HashFn,KickingStrat)                           \
  using MURMUR1 = hashing::MurmurFinalizer<Key>; \
  using KapilCuckooHashTableFile##BucketSize##OverAlloc##HashFn##KickingStrat = kapilhashtable::KapilCuckooHashTableFile<Key, Payload, BucketSize,OverAlloc, HashFn, MURMUR1,KickingStrat>; \
  KAPILBMCuckoo(KapilCuckooHashTableFile##BucketSize##OverAlloc##HashFn##KickingStrat);

#define BenchmarKapilCuckooModel(BucketSize,OverAlloc,Model,KickingStrat1)                           \
  using MURMUR1 = hashing::MurmurFinalizer<Key>; \
  using KapilCuckooModelHashTable##BucketSize##OverAlloc##HashFn##KickingStrat1 = kapilmodelhashtable::KapilCuckooModelHashTable<Key, Payload, BucketSize,OverAlloc, Model, MURMUR1,KickingStrat1>; \
  KAPILBMCuckoo(KapilCuckooModelHashTable##BucketSize##OverAlloc##HashFn##KickingStrat1);

#define BenchmarKapilCuckooModelFile(BucketSize,OverAlloc,Model,KickingStrat1)                           \
  using MURMUR1 = hashing::MurmurFinalizer<Key>; \
  using KapilCuckooModelHashTableFile##BucketSize##OverAlloc##HashFn##KickingStrat1 = kapilmodelhashtable::KapilCuckooModelHashTableFile<Key, Payload, BucketSize,OverAlloc, Model, MURMUR1,KickingStrat1>; \
  KAPILBMCuckoo(KapilCuckooModelHashTableFile##BucketSize##OverAlloc##HashFn##KickingStrat1);

#define BenchmarKapilCuckooExotic(BucketSize,OverAlloc,MMPHF,KickingStrat1)                           \
  using MURMUR1 = hashing::MurmurFinalizer<Key>; \
  using KapilCuckooModelHashTable##BucketSize##OverAlloc##HashFn##KickingStrat1 = kapilcuckooexotichashtable::KapilCuckooExoticHashTable<Key, Payload, BucketSize,OverAlloc, MMPHF, MURMUR1,KickingStrat1>; \
  KAPILBMCuckoo(KapilCuckooModelHashTable##BucketSize##OverAlloc##HashFn##KickingStrat1);
// ########################### END CUCKOO HASHING ##############################







// ############################## SINGLE LUDOTABLE ##############################
// ############################## SINGLE LUDOTABLE ##############################
// ############################## SINGLE LUDOTABLE ##############################
template <class Table, size_t RangeSize>
static void PointLookupLudo(benchmark::State& state) {
  // Extract variables
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
  const auto succ_probability =
      static_cast<size_t>(state.range(3));

  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(RangeSize) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);
  std::vector<std::pair<Key, Payload>> data{};
  if (previous_signature != signature) {
    std::cout << "performing setup... ... ..";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);

      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      // int succ_probability=100;
      probing_set = dataset::generate_probing_set(keys, probing_dist,succ_probability);
    }

    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      std::cout << "failed" << std::endl;
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;

  }
  

  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;

  if (previous_signature != signature)
  {
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
    table->print_data_statistics();
  }

  previous_signature = signature;  

  size_t i = 0;
  Payload v = 0;
  masters_thesis::LudoTable<Key, Payload> cp(data);
  masters_thesis::DataPlaneLudo<Key, Payload> dp(cp);
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const Key searched = probing_set[i++];
    
    dp.lookUp(searched, v); 
    //benchmark::DoNotOptimize(it);
    
  }

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;

  std::stringstream ss;
  ss << succ_probability;
  std::string temp = ss.str();
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist)+":"+temp);
}


template <class Table, size_t RangeSize>
static void PointLookupLudoFile(benchmark::State& state) {
  // Extract variables
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
  const auto succ_probability =
      static_cast<size_t>(state.range(3));
  
  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(RangeSize) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);

  uint32_t blockSize(512);
  std::string filepath = "../datasets/oneFile.txt";
  filestore::Storage stoc(filepath, dataset_size, blockSize); //filename & ds_size, blocksize could be ignored.

  std::vector<std::pair<Key, Payload>> data{};
  if (previous_signature != signature) {
    std::cout << "performing setup... ... ..";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);
      size_t index = 0;
      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [&index](const Key& key) { return std::make_pair(key, index ++); });
      // int succ_probability=100;
      probing_set = dataset::generate_probing_set(keys, probing_dist, succ_probability);
    }
    
    if (data.empty()) {
      // otherwise google benchmark produces an error ;(
      for (auto _ : state) {
      }
      std::cout << "failed" << std::endl;
      return;
    }

    // build table
    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(data);
    free_lambda = []() { delete ((Table*)prev_table); };

    // measure time elapsed
    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds"
              << std::endl;

    // std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
    // prev_table->print_data_statistics();
  }
  
  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;

  if (previous_signature != signature) {
    std::cout<<std::endl<<"Dataset Size: "<<std::to_string(dataset_size) <<" Dataset: "<< dataset::name(did)<<std::endl;
    table->print_data_statistics();
  }

  // std::cout<<"signature swap"<<std::endl;
  previous_signature = signature;  
  
  size_t i = 0;
  Payload pos = 0;
  vector<char> value(blockSize);
  
  masters_thesis::LudoTable<Key, Payload> cp(data);
  masters_thesis::DataPlaneLudo<Key, Payload> dp(cp);
  //dp = new masters_thesis::DataPlaneLudo<Key, Payload>(cp);
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const auto searched = probing_set[i++];

    // point lookup
    auto it = dp.lookUp(searched, pos);  
    stoc.stocRead(pos, value);

    benchmark::DoNotOptimize(it);
    // __sync_synchronize();
    // full_mem_barrier;
  }

  // set counters (don't do this in inner loop to avoid tainting results)
  state.counters["table_bytes"] = table->byte_size();
  state.counters["table_directory_bytes"] = table->directory_byte_size();
  state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
  state.counters["data_elem_count"] = dataset_size;

  std::stringstream ss;
  ss << succ_probability;
  std::string temp = ss.str();
  state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                 dataset::name(probing_dist)+":"+temp);

}

#define KAPILBMLudo(Table)                                                         \
  BENCHMARK_TEMPLATE(PointLookupLudo, Table, 0)                                     \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions})                \
      ->Iterations(200000);

#define KAPILBMLudoFile(Table)                                                    \
  BENCHMARK_TEMPLATE(PointLookupLudoFile, Table, 0)                                \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions})              \
      ->Iterations(200000);

#define BenchmarKapilLudo()                                                         \
  using KapilLudoTable = LudoTable<Key, Payload>;                                  \
  KAPILBMLudo(KapilLudoTable);

#define BenchmarKapilLudoFile()                                                    \
  using KapilLudoTable = LudoTable<Key, Payload>;                                  \
  KAPILBMLudoFile(KapilLudoTable);
// ########################## END SINGLE LUDO TABLE ##########################




size_t extractBars(std::vector<Key>& keys, std::vector<Key>& bars, 
                      std::vector<std::vector<Key>>& dataPieces, 
                      size_t tableCapacity) {
  std::vector<Key> vectmp;
  sort(keys.begin(), keys.end());
  for (uint32_t i = 0; i < keys.size(); i++) {
    if (i % tableCapacity == 0 && i > 0) {
      bars.push_back(keys[i]);
      dataPieces.push_back(vectmp);
      vectmp.clear();
    }
    vectmp.push_back(keys[i]);
  }
  if (!vectmp.empty()) {
    dataPieces.push_back(vectmp);
  }
  return bars.size();
}


// ############################## MULTIPLE LUDO TABLE ##############################
// ############################## MULTIPLE LUDO TABLE ##############################
// ############################## MULTIPLE LUDO TABLE ##############################
template <class Table, size_t tableCapacity>
static void PointLookupMultipleLudo(benchmark::State& state) {
  const auto dataset_size = static_cast<size_t>(state.range(0));
  const auto did = static_cast<dataset::ID>(state.range(1));
  const auto probing_dist =
      static_cast<dataset::ProbingDistribution>(state.range(2));
  const auto succ_probability =
      static_cast<size_t>(state.range(3));
  std::string signature =
      std::string(typeid(Table).name()) + "_" + std::to_string(tableCapacity) +
      "_" + std::to_string(dataset_size) + "_" + dataset::name(did) + "_" +
      dataset::name(probing_dist);
  
  std::vector<std::pair<Key, Payload>> data{};
  std::vector<Key> bars{0};
  std::vector<std::vector<Key>> dataPieces{};

  if (previous_signature != signature) {
    std::cout << "performing setup... ... ..";
    auto start = std::chrono::steady_clock::now();

    // Generate data (keys & payloads) & probing set
    data.reserve(dataset_size);
    {
      auto keys = dataset::load_cached<Key>(did, dataset_size);

      std::transform(
          keys.begin(), keys.end(), std::back_inserter(data),
          [](const Key& key) { return std::make_pair(key, key - 5); });
      extractBars(keys, bars, dataPieces, tableCapacity);
      probing_set = dataset::generate_probing_set(keys, probing_dist,succ_probability);
    }

    if (data.empty()) {
      for (auto _ : state) {
      }
      std::cout << "failed" << std::endl;
      return;
    }

    if (prev_table != nullptr) free_lambda();
    prev_table = new Table(bars, dataPieces);
    free_lambda = []() { delete ((Table*)prev_table); };

    const auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;
    std::cout << "succeeded in " << std::setw(9) << diff.count() << " seconds" << std::endl;
  }

  assert(prev_table != nullptr);
  Table* table = (Table*)prev_table;

  previous_signature = signature;  

  size_t i = 0;
  Payload v = 0;
  for (auto _ : state) {
    while (unlikely(i >= probing_set.size())) i -= probing_set.size();
    const Key searched = probing_set[i++];
    
    table->lookup(searched, v);
    std::cout << "out is: " << v << std::endl;
  }

  table->print_data_statistics();

  if (false) {
    state.counters["table_bytes"] = table->byte_size();
    state.counters["table_directory_bytes"] = table->directory_byte_size();
    state.counters["table_bits_per_key"] = 8. * table->byte_size() / dataset_size;
    state.counters["data_elem_count"] = dataset_size;

    std::stringstream ss;
    ss << succ_probability;
    std::string temp = ss.str();
    state.SetLabel(table->name() + ":" + dataset::name(did) + ":" +
                    dataset::name(probing_dist)+":"+temp);
  }

}

#define KAPILBMMultipleLudo(Table)                                                         \
  BENCHMARK_TEMPLATE(PointLookupMultipleLudo, Table, 900)                                 \
      ->ArgsProduct({dataset_sizes, datasets, probe_distributions})                         \
      ->Iterations(20);

#define BenchmarKapilMultipleLudo()                                                         \
  using KapilMultiLudoTable = MultiLudoTable<Key, Payload>;                                  \
  KAPILBMMultipleLudo(KapilMultiLudoTable);


// ############################ END MULTIPLE LUDO TABLE ##########################







}  // namespace _
