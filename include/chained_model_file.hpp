#pragma once

#define _GNU_SOURCE
#include <immintrin.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <iterator>
#include <learned_hashing.hpp>
#include <limits>
#include <string>
#include <utility>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>

#include <fcntl.h>
#include <unistd.h>
#include <cstdint>

#include "include/blob/json.hpp"
#include "include/blob/filestore.hpp"
#include "include/convenience/builtins.hpp"
#include "include/support.hpp"

namespace masters_thesis {
template <class Key, class Payload, size_t BucketSize, size_t OverAlloc,
          class Model = learned_hashing::MonotoneRMIHash<Key, 1000000>,
          bool ManualPrefetch = false,
          Key Sentinel = std::numeric_limits<Key>::max(),
          uint32_t blockSize = 512>
class KapilChainedModelHashTableFile {
    
  public:
    Model model;
    int numLock = 0;
    uint32_t disktime = 0;
    uint32_t lookupCall = 0;
    std::vector<char> value;
    std::recursive_mutex locks[8192];
    std::string filepath = "../datasets/oneFile.txt";
    filestore::Storage *stoc;

  std::vector<Key> key_vec;  

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

  // directory of buckets
  std::vector<Bucket> buckets;

  // allocator for buckets
  std::unique_ptr<support::Tape<Bucket>> tape;

  /* Inserts a given (key,payload) tuple into the hashtable.
   *
   * Note that this function has to be private for now since
   * model retraining will be necessary if this is used as a
   * construction interface.
   */
  forceinline void insert(const Key& key, const Payload& payload) {
    
    const auto index = model(key)%buckets.size();
    //stoc->stocInsert(payload, value);
    assert(index >= 0);
    assert(index < buckets.size());
    buckets[index].insert(key, payload, *tape);
  }

 public:
  KapilChainedModelHashTableFile() = default;

  /**
   * Constructs a KapilChainedModelHashTableFile given a list of keys
   * together with their corresponding payloads
   */
  KapilChainedModelHashTableFile(std::vector<std::pair<Key, Payload>> data,
                                std::string fileName = "../datasets/oneFile.txt")
      : tape(std::make_unique<support::Tape<Bucket>>()) {

    stoc = new filestore::Storage(fileName, data.size(), blockSize);
    value.resize(blockSize, 'a');

    if (OverAlloc<10000) {
      buckets.resize((1 + data.size()*(1.00+(OverAlloc/100.00))) / BucketSize); 
    } else {
      buckets.resize((1 + data.size()*(((OverAlloc-10000)/100.00)) / BucketSize)); 
    }         
    
    // ensure data is sorted
    std::sort(data.begin(), data.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // obtain list of keys -> necessary for model training
    std::vector<Key> keys;
    keys.reserve(data.size());
    std::transform(data.begin(), data.end(), std::back_inserter(keys),
                   [](const auto& p) { return p.first; });

    // train model on sorted data
    model.train(keys.begin(), keys.end(), buckets.size());
    

    std::string model_name=model.name();

    std::cout<<std::endl<<"Start Here "<<BucketSize<<" "<<OverAlloc<<" "<<model.name()<<" Model Chained Balanced 0 "<<model.model_count()<<" 0"<<std::endl<<std::endl;

    key_vec.resize(2*keys.size(),0);
    std::sort(keys.begin(),keys.end());
    for(int i=0;i<keys.size();i++)
    {
      key_vec[2*i]=keys[i];
      key_vec[2*i+1]=1;
    }

    // std::cout<<key_vec.size()<<std::endl;
    // insert all keys according to model prediction.
    // since we sorted above, this will permit further
    // optimizations during lookup etc & enable implementing
    // efficient iterators in the first place.
    // for (const auto& d : data) insert(d.first, d.second);
    
    // std::random_shuffle(data.begin(), data.end());

    std::cout<<"Start Inserts"<<std::endl;
    auto start = std::chrono::high_resolution_clock::now(); 
    for(uint64_t i=0;i<data.size();i++) {
      insert(data[i].first, i);
    }
    auto stop = std::chrono::high_resolution_clock::now(); 

    std::cout<<"Ended Inserts"<<std::endl;
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); 
    std::cout<< std::endl << "Insert Latency is: "<< duration.count()*1.00/data.size() << " nanoseconds" << std::endl;

  }

  class Iterator {
    size_t directory_ind, bucket_ind;
    Bucket const* current_bucket;
    const KapilChainedModelHashTableFile& hashtable;

    Iterator(size_t directory_ind, size_t bucket_ind, Bucket const* bucket,
             const KapilChainedModelHashTableFile& hashtable)
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

    forceinline bool operator==(const Iterator& other) const {
      return directory_ind == other.directory_ind &&
             bucket_ind == other.bucket_ind &&
             current_bucket == other.current_bucket &&
             &hashtable == &other.hashtable;
    }

    friend class KapilChainedModelHashTableFile;
  };


  uint64_t rmi_range_query(uint64_t low_key,uint64_t high_key) {
    // std::cout<<"low high: "<<low_key<<" "<<high_key<<std::endl;
    uint64_t ans=0;
    uint64_t index=(model(low_key)*1.00/buckets.size())*(key_vec.size()/2);
    index=2*index;

    // std::cout<<"index: "<<index<<" curr val: "<<key_vec[index]<<std::endl;

    if(key_vec[index]<low_key)
    {
      while(key_vec[index]<low_key)
      {
        index+=2;
      }
    }
    else
    {
      while(key_vec[index-2]>low_key)
      {
        index-=2;
      }
    }

    // std::cout<<"index adjusted: "<<index<<" "<<std::endl;

    // int scan_keys=0;
    
    while(true)
    {
      if(key_vec[index]>high_key)
      {
        break;
      }
      else
      {
        ans+=key_vec[index+1];
        index+=2;
      }
      // scan_keys+=2;
    }

    // std::cout<<scan_keys<<std::endl;

    return  ans;

  }

  uint64_t rmi_point_query(uint64_t low_key) {
    // std::cout<<"low high: "<<low_key<<" "<<std::endl;
    uint64_t ans=0;
    uint64_t index=(model(low_key)*(key_vec.size()/2.00)/buckets.size());
    index=2*index;

    // std::cout<<"index: "<<index<<" curr val: "<<key_vec[index]<<std::endl;

    if(key_vec[index]<low_key) {
      while(key_vec[index]<low_key) {
        index+=2;
      }
    } else {
      while(key_vec[index-2]>low_key) {
        index-=2;
      }
    }

    // std::cout<<"index adjusted: "<<index<<" "<<std::endl<<std::endl;
    // int scan_keys=0;

    if(key_vec[index]==low_key) {
      return 1;
    } else {
      return 0;
    }
    

    // std::cout<<scan_keys<<std::endl;

    return  ans;

  }

  uint64_t hash_range_query(uint64_t low_key,uint64_t high_key)
  {
    uint64_t ans=0;
    uint64_t directory_ind=model(low_key);

    int exit_check=0;

    while(true)
    {
        auto bucket = &buckets[directory_ind];
      
        while (bucket != nullptr) 
        {
            for (size_t i = 0; i < BucketSize; i++) 
            {
                const auto& current_key = bucket->keys[i];
                if (current_key == Sentinel) break;
                if (current_key >= low_key && current_key <= high_key) 
                {
                  ans+=bucket->payloads[0];
                  // std::cout<<"bucket count: "<<bucket_count<<std::endl;
                  // return {directory_ind, i, bucket, *this};
                }
                if(current_key>high_key && current_key!=Sentinel)
                {
                  exit_check=1;
                  break;
                }
            }
            // std::cout<<"bucket: "<<directory_ind<<" "<<bucket->keys[0]<<" low: "<<low_key<<" high: "<<high_key<<std::endl;
            // const auto& current_key = bucket->keys[0];
            // if (current_key >= low_key && current_key <= high_key) 
            // {
            //   ans+=bucket->payloads[0];
            // }

            // if(current_key>high_key && current_key!=Sentinel)
            // {
            //   exit_check=1;
            //   break;
            // }

            if(exit_check==1)
            {
              break;
            }
            
            // bucket_count++;
            bucket = bucket->next;
        //   prefetch_next(bucket);
        }

        if(exit_check==1)
        {
          break;
        }

        directory_ind++;

    }

    return  ans;

  }



  int useless_func() {
    return 0;
  }

  void gap_stats() {
    std::vector<uint64_t> key_vec;
    std::vector<double> cdf_prediction,gap;
    std::map<int,uint64_t> count_gap;

    for(uint64_t buck_ind=0;buck_ind<buckets.size();buck_ind++) {
      auto bucket = &buckets[buck_ind%buckets.size()];

      while (bucket != nullptr) {
        for (size_t i = 0; i < BucketSize; i++) {
          const auto& current_key = bucket->keys[i];
          if (current_key == Sentinel) break;
          key_vec.push_back(current_key);
          cdf_prediction.push_back(model.double_prediction(current_key));
        }
        // bucket_count++;
        bucket = bucket->next;
        //   prefetch_next(bucket);
      }

    }  

    std::sort(key_vec.begin(),key_vec.end());
    std::sort(cdf_prediction.begin(),cdf_prediction.end());

    for(int i=0;i<cdf_prediction.size()-1;i++)
    {
      gap.push_back(cdf_prediction[i+1]-cdf_prediction[i]);
      gap[i]=gap[i]*key_vec.size();
    }

    for(int i=0;i<gap.size();i++)
    {
      int temp=ceil((gap[i]*50)-0.1);
      temp=temp*2;
      count_gap[temp]=0;
    }

    for(int i=0;i<gap.size();i++)
    {
      int temp=ceil((gap[i]*50)-0.1);
      temp=temp*2;
      count_gap[temp]++;
    }


    std::map<int, uint64_t>::iterator it;

    std::vector<uint64_t> vec_map_count(key_vec.size(),0);

    std::cout<<"Start MaxBucketSize Stats"<<std::endl;

    for(int i=0;i<cdf_prediction.size();i++)
    {
      uint64_t a=0;
      a=cdf_prediction[i]*key_vec.size();
      if (a<0)
      {
        a=0;
      }
      if (a>key_vec.size())
      {
        a=key_vec.size()-1;
      }
      vec_map_count[a]++;
    }

    uint64_t max_val=0;

    for(int i=0;i<vec_map_count.size();i++)
    {
      if(max_val<vec_map_count[i])
      {
        max_val=vec_map_count[i];
      }
    }  

    std::cout<<" max keys map to a location is: "<<max_val<<std::endl;



    std::cout<<"End MaxBucketSize Stats"<<std::endl;

    std::cout<<"Start Gap Stats"<<std::endl;

    for (it = count_gap.begin(); it != count_gap.end(); it++)
    {
      std::cout<<"Gap Stats: ";
      std::cout<<it->first<<" : "<<it->second<<std::endl;
        // std::cout << it->first    // string (key)
        //           << ':'
        //           << it->second   // string's value 
        //           << std::endl;
    }

    std::cout<<"End Gap Stats"<<std::endl;


    return;


  }


  void print_data_statistics(bool output = false) {
    std::vector<uint64_t> dist_from_ideal;
    //std::vector<uint64_t> dist_to_empty;

    std::map<int,int> dist_from_ideal_map;
    //std::map<int,int> dist_to_empty_map;

    for(uint64_t buck_ind=0;buck_ind<buckets.size();buck_ind++) {
      auto bucket = &buckets[buck_ind%buckets.size()];
      for (size_t i = 0; i < BucketSize; i++) {
        const auto& current_key = bucket->keys[i];
        if(current_key==Sentinel) {
          break;
        }

        size_t directory_ind = model(current_key)%(buckets.size());
        // std::cout<<" pred val: "<<directory_ind<<" key val: "<<current_key<<" bucket val: "<<buck_ind<<std::endl;
        dist_from_ideal.push_back(directory_ind-buck_ind);
      }
    }

    std::sort(dist_from_ideal.begin(),dist_from_ideal.end());

    for(int i=0;i<dist_from_ideal.size();i++) {
      dist_from_ideal_map[dist_from_ideal[i]]=0;
    }

    for(int i=0;i<dist_from_ideal.size();i++) {
      dist_from_ideal_map[dist_from_ideal[i]]+=1;
    }

    if (output) {
      std::ofstream outFile("../results/ideal_pos_diff_chain_model.json");
      if (outFile.is_open()) {
        for (auto it = dist_from_ideal_map.begin(); it != dist_from_ideal_map.end(); ++it) {
            outFile << it->first << "," << it->second << std::endl;
        }
        outFile.close();
      } else {
        std::cout << "Error opening file!" << std::endl;
      }
      outFile.close();
    }

  }



  /**
   * Past the end iterator, use like usual in stl
   */
  forceinline Iterator end() const {
    return {buckets.size(), 0, nullptr, *this};
  }

  forceinline int hash_val(const Key& key) {
    return model(key);
  }


  /**
   * Returns an iterator pointing to the payload for a given key
   * or end() if no such key could be found
   *
   * @param key the key to search
   */
  forceinline int operator[](const Key& key) {
    // assert(key != Sentinel);
    // obtain directory bucket
    size_t directory_ind = model(key)%buckets.size();
    int exit_check=0;
    
    while(true) {
      auto bucket = &buckets[directory_ind];
      while (bucket != nullptr) {
        for (size_t i = 0; i < BucketSize; i++) {
            const auto& current_key = bucket->keys[i];
            if (current_key == Sentinel) 
              break;
            if (current_key == key) {
              Payload payload = bucket->payloads[i];
              return 1;
              // ans+=bucket->payloads[0];
              // std::cout<<"bucket count: "<<bucket_count<<std::endl;
            }
            if (current_key > key) {
              exit_check=1;
              break;
            }
        }
        if (exit_check==1) {
          break;
        }
        bucket = bucket->next;
      }
      if (exit_check==1) {
        break;
      }
      directory_ind++;
    }
    return 0;

  }

  forceinline int lookUp(const Key& key, std::vector<char>& value) {
    // assert(key != Sentinel);
    // obtain directory bucket
    size_t directory_ind = model(key)%(buckets.size());
    int exit_check=0;
    
    while(true) {
      auto bucket = &buckets[directory_ind];
      while (bucket != nullptr) {
        for (size_t i = 0; i < BucketSize; i++) {
            const auto& current_key = bucket->keys[i];
            const Payload current_pos = bucket->payloads[i];
            if (current_key == Sentinel) 
              break;
            if (current_key == key) {
              //stoc->stocRead(current_pos, value);
              return 1;
            }
            if (current_key > key) {
              exit_check=1;
              break;
            }
        }
        if (exit_check==1) {
          break;
        }
        bucket = bucket->next;
      }
      if (exit_check==1) {
        break;
      }
      directory_ind++;
    }
    return 0;

  }


  std::string name() {
    std::string prefix = (ManualPrefetch ? "Prefetched" : "");
    return prefix + "KapilChainedModelHashTableFile<" + std::to_string(sizeof(Key)) + ", " +
           std::to_string(sizeof(Payload)) + ", " + std::to_string(BucketSize) +
           ", " + model.name() + ">";
  }

  size_t directory_byte_size() const {
    size_t directory_bytesize = sizeof(decltype(buckets));
    for (const auto& bucket : buckets) directory_bytesize += bucket.byte_size();
    return directory_bytesize;
  }

  //kapil_change: assuming model size to be zero  
  size_t model_byte_size() const { return model.byte_size(); }

  size_t byte_size() const { return model_byte_size() + directory_byte_size(); }
};
}  // namespace masters_thesis
