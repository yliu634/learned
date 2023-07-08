#pragma once

#include <algorithm>
#include <random>

#include "ycsbc/core_workload.h"
#include "ycsbc/utils.h"

namespace dataset {
enum class ProbingDistribution {
  /// every key has the same probability to be queried
  UNIFORM,

  /// probing skewed according to exponential distribution, i.e.,
  /// some keys k are way more likely to be picked than others.
  /// rank(k) directly correlates to the k' probability of being picked
  EXPONENTIAL_SORTED,

  /// probing skewed according to exponential distribution, i.e.,
  /// some keys k are way more likely to be picked than others.
  /// rank(k) does not influence the k's probability of being picked
  EXPONENTIAL_RANDOM,

  /// transfer wl with it.
  YCSB_C
};

inline std::string name(ProbingDistribution p_dist) {
  switch (p_dist) {
    case ProbingDistribution::UNIFORM:
      return "uniform";
    case ProbingDistribution::EXPONENTIAL_SORTED:
      return "exponential_sorted";
    case ProbingDistribution::EXPONENTIAL_RANDOM:
      return "exponential_random";
    case ProbingDistribution::YCSB_C:
      return "ycsb_c";
  }
  return "unnamed";
};

template<class T>
void probing_set_generate_ycsb(const std::string filepath, 
            std::vector<T>& probing_set, size_t size) {
  probing_set.clear();
  utils::Properties props;
  ifstream input(filepath);
  try {
    props.Load(input);
  } catch (const string &message) {
    cout << message << endl;
    exit(0);
  }
  input.close();
  props.SetProperty("recordcount", std::to_string(size));
  ycsbc::CoreWorkload wl;
  wl.Init(props);
  for (size_t i = 0; i < size; i++) {
    std::string strkey = wl.NextTransactionKey().substr(4, 16);
    probing_set.push_back(std::stoull(strkey));
  }
}


/**
 * generates a probing order for any dataset dataset, given a desired
 * distribution
 */
template <class T>
static std::vector<T> generate_probing_set(std::vector<T> dataset,
          ProbingDistribution distribution, int succ_probability) {
  if (dataset.empty()) return {};

  std::random_device rd;
  std::default_random_engine rng(rd());

  size_t size = dataset.size();
  std::vector<T> probing_set(size, dataset[0]);

  switch (distribution) {
    case ProbingDistribution::UNIFORM: {
      std::uniform_int_distribution<> dist(0, dataset.size());//(10000, dataset.size() - 10000);
      
      for (size_t i = 0; i < size; i++)
      { 
        int adder=0;
        if (((uint64_t)dist(rng)%100)>succ_probability)
        {
          adder=dist(rng)%331777;
        }
        probing_set[i] = dataset[dist(rng)]+adder;
      }
      break;
    }
    case ProbingDistribution::EXPONENTIAL_SORTED: {
      std::exponential_distribution<> dist(10);

      for (size_t i = 0; i < size; i++){
        int adder=0;
        if (((uint64_t)dist(rng)%100)>succ_probability)
        {
          adder=1;
        }
        probing_set[i] =
            dataset[(dataset.size() - 1) * std::min(1.0, dist(rng))]+adder;
      }
      break;
    }
    case ProbingDistribution::EXPONENTIAL_RANDOM: {
      // shuffle to avoid skewed results for sorted data, i.e., when
      // dataset is sorted, this will always prefer lower keys. This
      // might make a difference for tries, e.g. when they are left deep
      // vs right deep!
      std::shuffle(dataset.begin(), dataset.end(), rng);

      std::exponential_distribution<> dist(10);

      for (size_t i = 0; i < size; i++){
        int adder=0;
        if (((uint64_t)dist(rng)%100)>succ_probability) {
          adder=1;
        }
        probing_set[i] =
            dataset[(dataset.size() - 1) * std::min(1.0, dist(rng))]+adder;
      }
      break;
    }
    case ProbingDistribution::YCSB_C: {
      //because every time with you use same configured ycsb::wl can generate same keys
      probing_set_generate_ycsb<T>("../ycsbc/workloads/workloadc.spec", probing_set, size);
      break;
    }
  }

  return probing_set;
}

/**
 * generates a probing order for any dataset dataset, given a desired
 * distribution
 */
template <class T>
static std::vector<T> generate_insert_set(std::vector<T> dataset,
          ProbingDistribution distribution, int succ_probability, uint32_t insert_size = 1000000) {
  if (dataset.empty()) return {};

  std::random_device rd;
  std::default_random_engine rng(rd());

  //size_t size = dataset.size();
  size_t size = insert_size;
  std::vector<T> probing_set(size, dataset[0]);

  switch (distribution) {
    case ProbingDistribution::UNIFORM: {
      std::uniform_int_distribution<> dist(0, dataset.size());//(10000, dataset.size() - 10000);
      
      for (size_t i = 0; i < size; i++)
      { 
        int adder=0;
        if (((uint64_t)dist(rng)%100)>succ_probability)
        {
          adder=dist(rng)%331777;
        }
        probing_set[i] = dataset[dist(rng)]+adder;
      }
      break;
    }
    case ProbingDistribution::EXPONENTIAL_SORTED: {
      std::exponential_distribution<> dist(10);

      for (size_t i = 0; i < size; i++){
        int adder=0;
        if (((uint64_t)dist(rng)%100)>succ_probability)
        {
          adder=1;
        }
        probing_set[i] =
            dataset[(dataset.size() - 1) * std::min(1.0, dist(rng))]+adder;
      }
      break;
    }
    case ProbingDistribution::EXPONENTIAL_RANDOM: {
      // shuffle to avoid skewed results for sorted data, i.e., when
      // dataset is sorted, this will always prefer lower keys. This
      // might make a difference for tries, e.g. when they are left deep
      // vs right deep!
      std::shuffle(dataset.begin(), dataset.end(), rng);

      std::exponential_distribution<> dist(10);

      for (size_t i = 0; i < size; i++){
        int adder=0;
        if (((uint64_t)dist(rng)%100)>succ_probability) {
          adder=1;
        }
        probing_set[i] =
            dataset[(dataset.size() - 1) * std::min(1.0, dist(rng))]+adder;
      }
      break;
    }
    case ProbingDistribution::YCSB_C: {
      //because every time with you use same configured ycsb::wl can generate same keys
      probing_set_generate_ycsb<T>("../ycsbc/workloads/workloadc.spec", probing_set, size);
      break;
    }
  }

  return probing_set;
}
}  // namespace dataset
