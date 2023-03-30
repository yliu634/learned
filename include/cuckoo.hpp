#pragma once

// This is a modified version of hashing.cc from SOSD repo (https://github.com/learnedsystems/SOSD),
// originally taken from the Stanford FutureData index baselines repo. Original copyright:
// Copyright (c) 2017-present Peter Bailis, Kai Sheng Tai, Pratiksha Thaker, Matei Zaharia
// MIT License
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

#include "convenience/builtins.hpp"

namespace kapilhashtable {
   /**
    * Place entry in bucket with more available space.
    * If both are full, kick from either bucket with 50% chance
    */
   struct KapilBalancedKicking {
     private:
      std::mt19937 rand_;

     public:
      static std::string name() {
         return "balanced_kicking";
      }

      template<class Bucket, class Key, class Payload, size_t BucketSize, Key Sentinel>
      forceinline std::optional<std::pair<Key, Payload>> operator()(Bucket* b1, Bucket* b2, const Key& key,
                                                                    const Payload& payload) {
         size_t c1 = 0, c2 = 0;
         for (size_t i = 0; i < BucketSize; i++) {
            c1 += (b1->slots[i].key == Sentinel ? 0 : 1);
            c2 += (b2->slots[i].key == Sentinel ? 0 : 1);
         }

         if (c1 <= c2 && c1 < BucketSize) {
            b1->slots[c1] = {.key = key, .payload = payload};
            return std::nullopt;
         }

         if (c2 < BucketSize) {
            b2->slots[c2] = {.key = key, .payload = payload};
            return std::nullopt;
         }

         const auto rng = rand_();
         const auto victim_bucket = rng & 0x1 ? b1 : b2;
         const size_t victim_index = rng % BucketSize;
         Key victim_key = victim_bucket->slots[victim_index].key;
         Payload victim_payload = victim_bucket->slots[victim_index].payload;
         victim_bucket->slots[victim_index] = {.key = key, .payload = payload};
         return std::make_optional(std::make_pair(victim_key, victim_payload));
      };
   };

   /**
    * if primary bucket has space, place entry in there
    * else if secondary bucket has space, place entry in there
    * else kick a random entry from the primary bucket with chance
    *
    * @tparam Bias chance that element is kicked from second bucket in percent (i.e., value of 10 -> 10%)
    */
   template<uint8_t Bias>
   struct KapilBiasedKicking {
     private:
      std::mt19937 rand_;
      double chance = static_cast<double>(Bias) / 100.0;
      uint32_t threshold_ = static_cast<uint32_t>(static_cast<double>(std::numeric_limits<uint32_t>::max()) * chance);

     public:
      static std::string name() {
         return "biased_kicking_" + std::to_string(Bias);
      }

      template<class Bucket, class Key, class Payload, size_t BucketSize, Key Sentinel>
      forceinline std::optional<std::pair<Key, Payload>> operator()(Bucket* b1, Bucket* b2, const Key& key,
                                                                    const Payload& payload) {
         size_t c1 = 0, c2 = 0;
         for (size_t i = 0; i < BucketSize; i++) {
            c1 += (b1->slots[i].key == Sentinel ? 0 : 1);
            c2 += (b2->slots[i].key == Sentinel ? 0 : 1);
         }

         if (c1 < BucketSize) {
            b1->slots[c1] = {.key = key, .payload = payload};
            return std::nullopt;
         }

         if (c2 < BucketSize) {
            b2->slots[c2] = {.key = key, .payload = payload};
            return std::nullopt;
         }

         const auto rng = rand_();
         const auto victim_bucket = rng > threshold_ ? b1 : b2;
         const size_t victim_index = rng % BucketSize;
         Key victim_key = victim_bucket->slots[victim_index].key;
         Payload victim_payload = victim_bucket->slots[victim_index].payload;
         victim_bucket->slots[victim_index] = {.key = key, .payload = payload};
         return std::make_optional(std::make_pair(victim_key, victim_payload));

      };
   };

   /**
    * if primary bucket has space, place entry in there
    * else if secondary bucket has space, place entry in there
    * else kick a random entry from the primary bucket and place entry in primary bucket
    */
   using KapilUnbiasedKicking = KapilBiasedKicking<0>;

   template<class Key, class Payload, size_t BucketSize, size_t OverAlloc, class HashFn1, class HashFn2, 
      class KickingFn, Key Sentinel = std::numeric_limits<Key>::max()>
    class KapilCuckooHashTable {
     public:
      using KeyType = Key;
      using PayloadType = Payload;

     private:
      const size_t MaxKickCycleLength;
      const HashFn1 hashfn1;
      const HashFn2 hashfn2;
      // const ReductionFn1 reductionfn1;
      // const ReductionFn2 reductionfn2;
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
      KapilCuckooHashTable(std::vector<std::pair<Key, Payload>> data)
         : MaxKickCycleLength(50000) {

         /*if (OverAlloc<10000) {
            buckets.resize((1 + data.size()*(1.00+(OverAlloc/100.00))) / BucketSize); 
         } else {
            buckets.resize((1 + data.size()*(((OverAlloc-10000)/100.00)) / BucketSize)); 
         }*/
         buckets.resize((1 + data.size()*(1.00+(OverAlloc/100.00))) / BucketSize); 

         std::sort(data.begin(), data.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

         // obtain list of keys -> necessary for model training
         std::vector<Key> keys;
         keys.reserve(data.size());
         std::transform(data.begin(), data.end(), std::back_inserter(keys),
                        [](const auto& p) { return p.first; });

         auto start = std::chrono::high_resolution_clock::now(); 

         for(uint64_t i=0;i<data.size();i++) {
            insert(data[i].first, i);
            std::cout << "This is the " << i << std::endl;
         }

         auto stop = std::chrono::high_resolution_clock::now(); 
         auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); 
         std::cout<< std::endl << "Insert Latency is: "<< duration.count()*1.00/data.size() << " nanoseconds" << std::endl;

      }

      int lookup(const Key& key) const {
         const auto h1 = hashfn1(key);
         const auto i1 = h1%buckets.size();

         const Bucket* b1 = &buckets[i1];
         for (size_t i = 0; i < BucketSize; i++) {
            if (b1->slots[i].key == key) {
               Payload payload = b1->slots[i].payload;
               return 1;
            }
         }

         auto i2 = (hashfn2(408957)^hashfn1(key))%buckets.size();
         if (i2 == i1) {
            i2 = (i1 == buckets.size() - 1) ? 0 : i1 + 1;
         }

         const Bucket* b2 = &buckets[i2];
         for (size_t i = 0; i < BucketSize; i++) {
            if (b2->slots[i].key == key) {
               Payload payload = b2->slots[i].payload;
               return 1;
            }
         }
         return 0;
      }

      int useless_func() {
      return 0;
      }


      void print_data_statistics() {
         size_t primary_key_cnt = 0;
         size_t total_cnt=0;

         for (uint64_t buck_ind=0;buck_ind<buckets.size();buck_ind++) {

            const Bucket* b1 = &buckets[buck_ind];
            for (size_t i = 0; i < BucketSize; i++) {
               if (b1->slots[i].key == Sentinel) {
                  break;
                  // return std::make_optional(payload);
               }
               size_t directory_ind = hashfn1(b1->slots[i].key)%(buckets.size());
               if(directory_ind==buck_ind) {
                  primary_key_cnt++;
               }
               total_cnt++;
            }
         } 

         std::cout<<" Primary Key Ratio: "<<primary_key_cnt*1.00/total_cnt<<std::endl;

         return;
      }


      std::map<std::string, std::string> lookup_statistics(const std::vector<Key>& dataset) const {
         size_t primary_key_cnt = 0;

         for (const auto& key : dataset) {
            const auto h1 = hashfn1(key);
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

      void insert(const Key& key, const Payload& value) {
         insert(key, value, 0);
      }

      static constexpr forceinline size_t bucket_byte_size() {
         return sizeof(Bucket);
      }

      static forceinline std::string name() {
         return "cuckoo_" + std::to_string(BucketSize) + "_" + KickingFn::name();
      }

      static forceinline std::string hash_name() {
         return HashFn1::name() + "-" + HashFn2::name();
      }

      // static forceinline std::string reducer_name() {
      //    return ReductionFn1::name() + "-" + ReductionFn2::name();
      // }

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
         // TODO: track max kick_count for result graphs
         if (kick_count > MaxKickCycleLength) {
            throw std::runtime_error("maximum kick cycle length (" + std::to_string(MaxKickCycleLength) + ") reached");
         }

         const auto h1 = hashfn1(key);
         const auto i1 = h1%buckets.size();
         auto i2 = (hashfn2(408957)^hashfn1(key))%buckets.size();

         // std::cout<<key<<" h1: "<<i1<<" h2: "<<i2<<" kick_count "<<kick_count<<" capacity: "<<buckets.size()<<std::endl;

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
