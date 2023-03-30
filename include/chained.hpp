#pragma once

#include <immintrin.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <iterator>
#include <learned_hashing.hpp>
#include <limits>
#include <string>
#include <utility>

#include "include/convenience/builtins.hpp"
#include "include/support.hpp"

namespace masters_thesis {
template <class Key, class Payload, size_t BucketSize, size_t OverAlloc,
          class HashFn,
          bool ManualPrefetch = false,
          Key Sentinel = std::numeric_limits<Key>::max()>
class KapilChainedHashTable {
    
    public:
      const HashFn hashfn;  

  struct Bucket {
    std::array<Key, BucketSize> keys;
    std::array<Payload, BucketSize> payloads;
    Bucket* next = nullptr;

    Bucket() {
      // Sentinel value in each slot per default
      std::fill(keys.begin(), keys.end(), Sentinel);
    }

    void insert(const Key& key, const Payload& payload,
                support::Tape<Bucket>& tape) {
      Bucket* previous = this;

      for (Bucket* current = previous; current != nullptr;
        current = current->next) {
        for (size_t i = 0; i < BucketSize; i++) {
          if (current->keys[i] == Sentinel) {
            current->keys[i] = key;
            current->payloads[i] = payload;
            return;
          }
        }
        previous = current;
      }

      // static var will be shared by all instances
      previous->next = tape.alloc();
      previous->next->insert(key, payload, tape);
    }

    size_t byte_size() const {
      return sizeof(Bucket) + (next != nullptr ? next->byte_size() : 0);
    }
  };

  /// directory of buckets
  std::vector<Bucket> buckets;

  /// model for predicting the correct index
  //   Model model;

  /// allocator for buckets
  std::unique_ptr<support::Tape<Bucket>> tape;

  /**
   * Inserts a given (key,payload) tuple into the hashtable.
   *
   * Note that this function has to be private for now since
   * model retraining will be necessary if this is used as a
   * construction interface.
   */
  forceinline void insert(const Key& key, const Payload& payload) {
    const auto index = hashfn(key)%buckets.size();
    assert(index >= 0);
    assert(index < buckets.size());
    buckets[index].insert(key, payload, *tape);
  }

 public:
  KapilChainedHashTable() = default;

  /**
   * Constructs a KapilChainedHashTable given a list of keys
   * together with their corresponding payloads
   */
  KapilChainedHashTable(std::vector<std::pair<Key, Payload>> data)
      :tape(std::make_unique<support::Tape<Bucket>>()) {
        
    if (OverAlloc<10000)
    {
      buckets.resize((1 + data.size()*(1.00+(OverAlloc/100.00))) / BucketSize); 
    } 
    else
    {
      buckets.resize((1 + data.size()*(((OverAlloc-10000)/100.00)) / BucketSize)); 
    }   

    std::cout<<std::endl<<"Start Here "<<BucketSize<<" "<<OverAlloc<<" "<<hashfn.name()<<" Traditional Chained Balanced 0 "<<0<<" 0"<<std::endl<<std::endl;
       
    // ensure data is sorted
    std::sort(data.begin(), data.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // obtain list of keys -> necessary for model training
    std::vector<Key> keys;
    keys.reserve(data.size());
    std::transform(data.begin(), data.end(), std::back_inserter(keys),
                   [](const auto& p) { return p.first; });

    // train model on sorted data
    // model.train(keys.begin(), keys.end(), buckets.size());

    // insert all keys according to model prediction.
    // since we sorted above, this will permit further
    // optimizations during lookup etc & enable implementing
    // efficient iterators in the first place.
    // for (const auto& d : data)
    // {
    //   insert(d.first, d.second);
    // }
    // we can train keys in order but insert them in a shuffled way
    std::random_shuffle(data.begin(), data.end());
    //uint64_t insert_count=1000000;
   
    auto start = std::chrono::high_resolution_clock::now(); 
    for(uint64_t i=0; i<data.size(); i++) {
      insert(data[i].first, i);
    }
    auto stop = std::chrono::high_resolution_clock::now(); 

    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); 
    std::cout<< std::endl << "Insert Latency is: "<< duration.count()*1.00/data.size() << " nanoseconds" << std::endl;

  }

  class Iterator {
    size_t directory_ind, bucket_ind;
    Bucket const* current_bucket;
    const KapilChainedHashTable& hashtable;

    Iterator(size_t directory_ind, size_t bucket_ind, Bucket const* bucket,
             const KapilChainedHashTable& hashtable)
        : directory_ind(directory_ind),
          bucket_ind(bucket_ind),
          current_bucket(bucket),
          hashtable(hashtable) {}

   public:
    forceinline const Key& key() const {
      assert(current_bucket != nullptr);
      return current_bucket->keys[bucket_ind];
    }

    forceinline const Payload& payload() const {
      assert(current_bucket != nullptr);
      return current_bucket->payloads[bucket_ind];
    }

    forceinline Iterator& operator++() {
      assert(current_bucket != nullptr);

      // prefetch next bucket during iteration. If there is no
      // next bucket, then prefetch the next directory slot
      if (current_bucket->next != nullptr)
        prefetch(current_bucket->next, 0, 0);
      else if (directory_ind + 1 < hashtable.buckets.size())
        prefetch(&hashtable.buckets[directory_ind + 1], 0, 0);

      // since data was inserted in sorted order,
      // simply advancing to next slot does the trick
      bucket_ind++;

      // switch to next bucket
      if (bucket_ind >= BucketSize ||
          current_bucket->keys[bucket_ind] == Sentinel) {
        bucket_ind = 0;
        current_bucket = current_bucket->next;

        // switch to next bucket chain in directory
        if (current_bucket == nullptr &&
            ++directory_ind < hashtable.buckets.size())
          current_bucket = &hashtable.buckets[directory_ind];
      }

      return *this;
    }

    // // TODO(dominik): support postfix increment
    // forceinline Iterator operator++(int) {
    //   Iterator old = *this;
    //   ++this;
    //   return old;
    // }

    forceinline bool operator==(const Iterator& other) const {
      return directory_ind == other.directory_ind &&
             bucket_ind == other.bucket_ind &&
             current_bucket == other.current_bucket &&
             &hashtable == &other.hashtable;
    }

    friend class KapilChainedHashTable;
  };



  void print_data_statistics(bool output = false) {
    std::vector<uint64_t> num_ele;
    std::map<int,int> num_ele_map;

    for(uint64_t buck_ind=0;buck_ind<buckets.size();buck_ind++) {
      auto bucket = &buckets[buck_ind%buckets.size()];

      int ele_count=0;

      while (bucket != nullptr) {
        for (size_t i = 0; i < BucketSize; i++) {
          const auto& current_key = bucket->keys[i];
          if (current_key == Sentinel) break;
          ele_count++;
        }
        bucket = bucket->next;
      }

      num_ele.push_back(ele_count);
    }

    std::sort(num_ele.begin(),num_ele.end());

    for(int i=0;i<num_ele.size();i++)
    {
      num_ele_map[num_ele[i]]=0;
    }

    for(int i=0;i<num_ele.size();i++)
    {
      num_ele_map[num_ele[i]]+=1;
    }


    std::map<int, int>::iterator it;

    std::cout<<"Start Num Elements"<<std::endl;

    for (it = num_ele_map.begin(); it != num_ele_map.end(); it++)
    {
      std::cout<<"Num Elements: ";
      std::cout<<it->first<<" : "<<it->second<<std::endl;
        // std::cout << it->first    // string (key)
        //           << ':'
        //           << it->second   // string's value 
        //           << std::endl;
    }

    return;

  }

  int useless_func() {
    return 0;
  }

  forceinline int hash_val(const Key& key) {
    return hashfn(key)%buckets.size();
  }


  /**
   * Past the end iterator, use like usual in stl
   */
  forceinline Iterator end() const {
    return {buckets.size(), 0, nullptr, *this};
  }

  /**
   * Returns an iterator pointing to the payload for a given key
   * or end() if no such key could be found
   *
   * @param key the key to search
   */
  forceinline int operator[](const Key& key) const {
    assert(key != Sentinel);

    // obtain directory bucket
    const size_t directory_ind = hashfn(key)%buckets.size();
    // std::cout<<"key: "<<key<<std::endl;
    

    auto bucket = &buckets[directory_ind];

    while (bucket != nullptr) {
      for (size_t i = 0; i < BucketSize; i++) {
        const auto& current_key = bucket->keys[i];
        if (current_key == Sentinel) 
          break;
        if (current_key == key) {
          Payload payload = bucket->payloads[i];
          return 1;
        }
      }
      bucket = bucket->next;
    }

    return 0;
    // return end();
  }

  std::string name() {
    std::string prefix = (ManualPrefetch ? "Prefetched" : "");
    return prefix + "KapilChainedHashTable<" + std::to_string(sizeof(Key)) + ", " +
           std::to_string(sizeof(Payload)) + ", " + std::to_string(BucketSize) +
           ", " + hashfn.name() + ">";
  }

  size_t directory_byte_size() const {
    size_t directory_bytesize = sizeof(decltype(buckets));
    for (const auto& bucket : buckets) directory_bytesize += bucket.byte_size();
    return directory_bytesize;
  }

  //kapil_change: assuming model size to be zero  
  size_t model_byte_size() const { return 0; }

  size_t byte_size() const { return model_byte_size() + directory_byte_size(); }
};

}  // namespace masters_thesis
