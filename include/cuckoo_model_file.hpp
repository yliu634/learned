#pragma once

// This is a modified version of hashing.cc from SOSD repo (https://github.com/learnedsystems/SOSD),
// originally taken from the Stanford FutureData index baselines repo. Original copyright:
// Copyright (c) 2017-present Peter Bailis, Kai Sheng Tai, Pratiksha Thaker, Matei Zaharia
// MIT License
#define _GNU_SOURCE
#include <immintrin.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <optional>
#include <random>
#include <vector>
#include <learned_hashing.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <cstdint>

#include "cuckoo_model.hpp"
#include "include/blob/json.hpp"
#include "include/blob/filestore.hpp"
#include "include/convenience/builtins.hpp"
#include "include/support.hpp"

namespace kapilmodelhashtable {

   using KapilModelUnbiasedKicking = KapilModelBiasedKicking<0>;

   template<class Key, class Payload, size_t BucketSize, size_t OverAlloc, class Model , class HashFn2, 
    class KickingFn, Key Sentinel = std::numeric_limits<Key>::max(),
          size_t blockSize = 512>
    class KapilCuckooModelHashTableFile {
     public:
      using KeyType = Key;
      using PayloadType = Payload;
      int numLock = 0;
      uint32_t disktime = 0;
      uint32_t lookupCall = 0;
      std::vector<char> value;
      std::recursive_mutex locks[8192];
      std::string filepath = "../datasets/oneFile.txt";
      filestore::Storage *stoc;

     private:
      const size_t MaxKickCycleLength;
      Model model;
      const HashFn2 hashfn2;
      KickingFn kickingfn;

      struct Bucket {
         struct Slot {
            Key key = Sentinel;
            Payload payload;
         } packed;

         std::array<Slot, BucketSize> slots;
      } packed;

      std::vector<Bucket> buckets;

      std::mt19937 rand_; // RNG for moving items around

     public:
      KapilCuckooModelHashTableFile(std::vector<std::pair<Key, Payload>> data,
                std::string fileName = "../datasets/oneFile.txt")
         : MaxKickCycleLength(50000) {

         stoc = new filestore::Storage(fileName, data.size(), blockSize);
         value.resize(blockSize, 'a');
         buckets.resize((1 + data.size()*(1.00+(OverAlloc/100.00))) / BucketSize);

         std::sort(data.begin(), data.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

         // obtain list of keys -> necessary for model training
         std::vector<Key> keys;
         keys.reserve(data.size());
         std::transform(data.begin(), data.end(), std::back_inserter(keys),
                        [](const auto& p) { return p.first; });

         // train model on sorted data
         model.train(keys.begin(), keys.end(), buckets.size());

         auto start = std::chrono::high_resolution_clock::now(); 
         for(uint64_t i=0;i<data.size();i++) {
            insert(data[i].first, i);
         }
         auto stop = std::chrono::high_resolution_clock::now(); 
         auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); 
         std::cout<< std::endl << "Insert Latency is: "<< duration.count()*1.00/data.size() << " nanoseconds" << std::endl;
      }

      int lookUp(const Key& key, std::vector<char>& value) {
         const auto h1 = model(key)%buckets.size();
         const auto i1 = h1%buckets.size();

         const Bucket* b1 = &buckets[i1];
         for (size_t i = 0; i < BucketSize; i++) {
            if (b1->slots[i].key == key) {
               Payload current_pos = b1->slots[i].payload;
               //stoc->stocRead(current_pos, value);
               return 1;
            }
         }
         auto i2 = (hashfn2(key))%buckets.size();
         if (i2 == i1) {
            i2 = (i1 == buckets.size() - 1) ? 0 : i1 + 1;
         }
         const Bucket* b2 = &buckets[i2];
         for (size_t i = 0; i < BucketSize; i++) {
            if (b2->slots[i].key == key) {
               Payload current_pos = b2->slots[i].payload;
               //stoc->stocRead(current_pos, value);
               return 1;
            }
         }
         return 0;
      }

      int useless_func() {
         return 0;
      }

      void print_data_statistics(bool output = false) {
         size_t primary_key_cnt = 0;
         size_t total_cnt=0;

         for(uint64_t buck_ind=0;buck_ind<buckets.size();buck_ind++) {

            const Bucket* b1 = &buckets[buck_ind];
            for (size_t i = 0; i < BucketSize; i++) {
               if (b1->slots[i].key == Sentinel) {
                  break;
               }
               size_t directory_ind = model(b1->slots[i].key)%(buckets.size());
               if(directory_ind==buck_ind) {
                  primary_key_cnt++;
               }
               total_cnt++;
            }
         }  
         std::cout<< "Primary Key Ratio: "<<primary_key_cnt*1.00/total_cnt<<std::endl;
         return ;
      }

      std::map<std::string, std::string> lookup_statistics(const std::vector<Key>& dataset) const {
         size_t primary_key_cnt = 0;

         for (const auto& key : dataset) {
            const auto h1 = model(key);
            const auto i1 = h1%buckets.size();

            const Bucket* b1 = &buckets[i1];
            for (size_t i = 0; i < BucketSize; i++)
               if (b1->slots[i].key == key)
                  primary_key_cnt++;
         }

         return {
            {"primary_key_ratio",
               std::to_string(static_cast<long double>(primary_key_cnt) / static_cast<long double>(dataset.size()))},
         };
      }

      void insert(const Key& key, const Payload& pos) {
         //stoc->stocInsert(pos, value);
         insert(key, pos, 0);
      }

      static constexpr forceinline size_t bucket_byte_size() {
         return sizeof(Bucket);
      }

      static forceinline std::string name() {
         return "cuckoo_" + std::to_string(BucketSize) + "_" + KickingFn::name();
      }

      static forceinline std::string hash_name() {
         return Model::name() + "-" + HashFn2::name();
      }

      static constexpr forceinline size_t bucket_size() {
         return BucketSize;
      }


      size_t directory_byte_size() const {
         size_t directory_bytesize = sizeof(decltype(buckets));
         for (const auto& bucket : buckets) directory_bytesize +=sizeof(Bucket);
         return directory_bytesize;
      }

      //kapil_change: assuming model size to be zero  
      size_t model_byte_size() const { return 0; }

      size_t byte_size() const { return model_byte_size() + directory_byte_size(); }

      // static constexpr forceinline size_t directory_address_count(const size_t& buckets.size()) {
      //    return (buckets.size() + BucketSize - 1) / BucketSize;
      // }

      void clear() {
         for (auto& bucket : buckets)
            for (auto& slot : bucket.slots)
               slot.key = Sentinel;
      }

      private:
      void insert(Key key, Payload payload, size_t kick_count) {
      start:
         if (kick_count > MaxKickCycleLength) {
            throw std::runtime_error("maximum kick cycle length (" + std::to_string(MaxKickCycleLength) + ") reached");
         }

         const auto h1 = model(key);
         const auto i1 = h1%buckets.size();
         auto i2 = (hashfn2(key))%buckets.size();

         if (unlikely(i2 == i1)) {
            i2 = (i1 == buckets.size() - 1) ? 0 : i1 + 1;
         }

         Bucket* b1 = &buckets[i1];
         Bucket* b2 = &buckets[i2];

         // Update old value if the key is already in the table
         for (size_t i = 0; i < BucketSize; i++) {
            if (b1->slots[i].key == key) {
               b1->slots[i].payload = payload;
               return;
            }
            if (b2->slots[i].key == key) {
               b2->slots[i].payload = payload;
               return;
            }
         }

         // Way to go Mr. Stroustrup
         if (const auto kicked =
                  kickingfn.template operator()<Bucket, Key, Payload, BucketSize, Sentinel>(b1, b2, key, payload)) {
            key = kicked.value().first;
            payload = kicked.value().second;
            kick_count++;
            goto start;
         }
      }
   };


} // namespace hashtable
