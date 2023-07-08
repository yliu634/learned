#pragma once

#include <immintrin.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <iterator>
#include <limits>
#include <string>
#include <utility>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <queue>
#include <learned_hashing.hpp>
#include <absl/hash/hash.h>

#include "ludo/common.h"
#include "ludo/hash.h"
#include "ludo/othello_cp_dp.h"
#include "ludo/cuckoo_ht.h"

#include "include/convenience/builtins.hpp"
#include "include/support.hpp"


// Class for efficiently storing key->value mappings when the size is
// known in advance and the keys are pre-hashed into uint64s.
// Keys should have "good enough" randomness (be spread across the
// entire 64 bit space).
//
// Important:  Clients wishing to use deterministic keys must
// ensure that their keys fall in the range 0 .. (uint64max-1);
// the table uses 2^64-1 as the "not occupied" flag.
//
// Inserted k must be unique, and there are no update
// or delete functions (until some subsequent use of this table
// requires them).
//
// Threads must synchronize their access to a PresizedHeadlessCuckoo.
//
// The cuckoo hash table is 4-way associative (each "bucket" has 4
// "slots" for key/value entries).  Uses breadth-first-search to find
// a good cuckoo path with less data movement (see
// http://www.cs.cmu.edu/~dga/papers/cuckoo-eurosys14.pdf )

namespace masters_thesis {

template<class K, class V, size_t OverAlloc, class Model, uint VL>
class ParrotDataPlane;


template<class K, class V, uint VL = sizeof(V) * 8>
class ParrotCommon {
public:
  static const uint8_t LocatorSeedLength = 5;
  static const uint MAX_REHASH = 2;
  
  static_assert(sizeof(V) * 8 >= VL);
  static const uint64_t ValueMask = (1ULL << VL) - 1;
  static const uint64_t VDMask = ValueMask;
  
  // The load factor is chosen slightly conservatively for speed and
  // to avoid the need for a table rebuild on insertion failure.
  // 0.94 is achievable, but 0.85 is faster and keeps the code simple
  // at the cost of a small amount of memory.
  // NOTE:  0 < kLoadFactor <= 1.0
  static constexpr double kLoadFactor = 0.90;
  static constexpr K Sentinel = std::numeric_limits<K>::max();
  static constexpr uint32_t Sentinel32 = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t Sentinel32Mask = (1UL << 31) - 1;
  // Cuckoo insert:  The maximum number of entries to scan should be ~400
  // (Source:  Personal communication with Michael Mitzenmacher;  empirical
  // experiments validate.).  After trying 400 candidate locations, declare
  // the table full - it's probably full of unresolvable cycles.  Less than
  // 400 reduces max occupancy;  much more results in very poor performance
  // around the full point.  For (2,4) a max BFS path len of 5 results in ~682
  // nodes to visit, calculated below, and is a good value.
  static constexpr uint8_t kMaxBFSPathLen = 5;
  
  static const uint8_t kSlotsPerBucket = 4;   // modification to this value leads to undefined behavior
  static const uint8_t kSlotPerLinknode = 4;
    
  uint32_t num_buckets_;
  AbslFastHasherTuple<K> h;
  AbslFastHasherTuple<K> digestH;
  
  virtual void setSeed(uint32_t s) {
    srand(s);
    h.setSeed(rand() | (uint64_t(rand()) << 32));
    digestH.setSeed(rand() | (uint64_t(rand()) << 32));
  }
  
};


// Cuckoo never failed to insert. if cuckoo rebuids for larger capacity, sync all data after the whole rebuild.
// Only consider concurrent update for othello locator
template<class K, class V, size_t OverAlloc, class Model = learned_hashing::MonotoneRMIHash<K, 1000000>, 
            uint VL = sizeof(V) * 8>
class ParrotControlPlane : public ParrotCommon<K, V, VL> {
public:
  using ParrotCommon<K, V, VL>::kSlotsPerBucket;
  using ParrotCommon<K, V, VL>::kLoadFactor;
  using ParrotCommon<K, V, VL>::kMaxBFSPathLen;
  
  using ParrotCommon<K, V, VL>::VDMask;
  using ParrotCommon<K, V, VL>::ValueMask;

  using ParrotCommon<K, V, VL>::LocatorSeedLength;
  using ParrotCommon<K, V, VL>::kSlotPerLinknode;

  using ParrotCommon<K, V, VL>::MAX_REHASH;
  using ParrotCommon<K, V, VL>::num_buckets_;
  using ParrotCommon<K, V, VL>::h;
  using ParrotCommon<K, V, VL>::digestH;
  using ParrotCommon<K, V, VL>::Sentinel;
  
  struct LinkNode {
    uint8_t seed = 0;
    uint8_t occupiedMask = 0;
    K bar;
    std::array<K, kSlotPerLinknode> keys;
    std::array<V, kSlotPerLinknode> values;
    //uint8_t fingerprints[kSlotsPerBucket];
    LinkNode* next = nullptr;
    LinkNode() {
      bar = Sentinel;
      std::fill(keys.begin(), keys.end(), Sentinel);
    }
    void insert(const K& key, const V& value, support::Tape<LinkNode>& tape) {
      LinkNode* previous = this;
      int8_t target_slot = -1;
      for (LinkNode* current = previous; current != nullptr; current = current->next) {
        for (size_t slot = 0; slot < kSlotPerLinknode; slot++) {
          if (occupiedMask & (1 << slot)) 
            continue;
          else if (target_slot == -1) {
            target_slot = slot;
            break;
          }
        }
        //std::cout << "current bar is: " << current->bar << std::endl;
        if (target_slot != -1) {
          if (key < current->bar) current->bar = key; // update bar
          current->keys[target_slot] = key;
          current->values[target_slot] = value;
          current->occupiedMask |= 1U << target_slot;

          AbslFastHasherTuple<K> h;
          bool occupied[4];
          for (uint8_t iSeed = 0; iSeed < 255; ++iSeed) {
            *(uint32_t *) occupied = 0U;
            h.setSeed(iSeed);
            //std::cout << "seed: " << unsigned(seed) << std::endl;
            bool success = true;
            for (char slot = 0; slot < kSlotPerLinknode; ++slot) {
              if (current->occupiedMask & (1 << slot)) {
                uint8_t i = uint8_t(h(current->keys[slot]) >> 6);
                if (occupied[i]) {
                  success = false;
                  break;
                } else { occupied[i] = true; }
              }
            }

            if (success) {
              LinkNode cpLinkNode = *(current); 
              current->occupiedMask = 0U;
              current->seed = iSeed;
              h.setSeed(iSeed);
              for (char slot = 0; slot < kSlotPerLinknode; ++slot) {
                if (cpLinkNode.occupiedMask & (1 << slot)) {
                  K moveKey = cpLinkNode.keys[slot];
                  uint8_t toSlot = uint8_t(h(moveKey) >> 6);
                  current->occupiedMask |= 1U << toSlot;
                  current->keys[toSlot] = moveKey;
                  current->values[toSlot] = cpLinkNode.values[slot];
                }
              }
              return;
            }
          }
          throw std::runtime_error("more than 255 times in separating keys in linknode");
          return;
        } 
        previous = current;
      }
      previous->next = tape.alloc();
      previous->next->insert(key, value, tape);
    }
  };

  // Buckets are organized with key_types clustered for access speed
  // and for compactness while remaining aligned.
  struct Bucket {
    uint8_t seed = 0;
    uint8_t occupiedMask = 0;
    K bar = Sentinel;
    K keys[kSlotsPerBucket];
    V values[kSlotsPerBucket];
    LinkNode* head = nullptr;

    inline bool isMember(K k) {
      for (int slot = 0; slot < kSlotsPerBucket; slot ++) {
        if (keys[slot] == k && (occupiedMask & (1 << slot))) return true;
      }
      return false;
    }
    inline bool insertLinkNode(const K &k, const V &v, support::Tape<LinkNode>& tape) {
      if (!head) {
        LinkNode* phead = new LinkNode();
        phead->next = head;
        head = phead;
      }
      head->insert(k, v, tape);
      return true;
    }
  } empty_bucket;
  
  uint32_t nKeys = 0;
  ControlPlaneOthello<K, uint8_t, 1, 0, true> locator;

  Model model;
  
  uint32_t capacity = 256U;
  std::unique_ptr<support::Tape<LinkNode>> tape;

  // Set upon initialization: num_entries / kLoadFactor / kSlotsPerBucket.
  std::vector<Bucket> buckets_, oldBuckets;

  virtual void setSeed(uint32_t s) {
    ParrotCommon<K, V, VL>::setSeed(s);
    locator.hab.setSeed(rand() | (uint64_t(rand()) << 32));
    locator.hd.setSeed(rand());
  }
  
  ParrotControlPlane(uint32_t capacity_)
      : locator(1U, true), nKeys(0), capacity(capacity_), tape(std::make_unique<support::Tape<LinkNode>>()) {
    num_buckets_ = 64U;
    buckets_.resize(num_buckets_, empty_bucket);
    //locator.resizeCapacity(capacity);
  }
  
  ~ParrotControlPlane(){
  }

  // The key type is fixed as a pre-hashed key for this specialized use.
  ParrotControlPlane(vector<pair<K, V>> &kv,
                  uint32_t capacity_ = 1024)
      : locator(1U, true), nKeys(0), capacity(capacity_), 
      tape(std::make_unique<support::Tape<LinkNode>>()) {
    
    num_buckets_ = 64U;
    num_buckets_ = (1 + kv.size()*(1.00+(OverAlloc/100.00))) / kSlotsPerBucket; 

    buckets_.resize(num_buckets_, empty_bucket);
    
    // ensure data is sorted
    std::sort(kv.begin(), kv.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // obtain list of keys -> necessary for model training
    std::vector<K> keys;
    keys.reserve(kv.size());
    std::transform(kv.begin(), kv.end(), std::back_inserter(keys),
                   [](const auto& p) { return p.first; });

    // train model on sorted data
    model.train(keys.begin(), keys.end(), buckets_.size());

    uint32_t toInsert = (uint32_t) kv.size();
    // locator.resizeCapacity(toInsert);
    // till this line, an empty ludo is ready
    if (toInsert) {
      for (int i = 0; i < toInsert; ++i) {
        insert(keys[i], i, false);
      }
    }
  }
  
  ParrotControlPlane(const vector<K> &keys, 
                  uint32_t capacity_ = 1024)
      : locator(1U, true), nKeys(0), capacity(capacity_),
        tape(std::make_unique<support::Tape<LinkNode>>()) {

    // num_buckets_ = 64U;
    num_buckets_ = (1 + keys.size()*(1.00+(OverAlloc/100.00))) / kSlotsPerBucket; 
    buckets_.resize(num_buckets_, empty_bucket);

    std::sort(keys.begin(), keys.end());

    // train model on sorted data
    model.train(keys.begin(), keys.end(), buckets_.size());

    uint32_t toInsert = (uint32_t) keys.size();
    // locator.resizeCapacity(toInsert);
    if (toInsert) {
      for (uint32_t i = 0; i <= keys.size(); i ++) {
        insert(keys[i], i, false);
      }
    }
  }
  
  inline uint32_t size() const {
    return nKeys;
  }
  
  // Insert return an int, 0 means successfully inserted in perfect hash slot
  // return 1 means we put it in over link node. -1 means fails.
  int insert(const K &k, V v, bool online = false) {
    // std::cout << "insert key: " << k << " value: " << v << std::endl;
    uint32_t bucketId = model(k) % buckets_.size();
    Bucket &bucket = buckets_[bucketId];
    if (bucket.isMember(k)) {
        //TODO:
    } else {
      int8_t target_bucket = -1;
      char target_slot = -1;
      for (char slot = 0; slot < kSlotsPerBucket; slot++) {
        if (bucket.occupiedMask & (1 << slot)) {
            continue;
        } else if (target_slot == -1) {
            target_slot = slot;
            break;
        }
      }

      bool succ;
      int result = 0;
      bool debug(false);
      if (target_slot != -1) {
        // if(full_debug) Clocker::count("direct insert");
        bucket.bar = k < bucket.bar? k:bucket.bar;
        succ = putItem(k, v, bucketId, target_slot);
        if (!succ) result = -2;   //did not find proper seed to seperate them
        if (debug) std::cout << "insert in bucket: " << bucketId  << " result: " << result << std::endl;
      } else {
        // if(full_debug) Clocker::count("overflow link insert");
        if (debug) std::cout << "insert in linklist: " << bucketId << std::endl;
        succ = bucket.insertLinkNode(k, v, *tape);
        if (!succ)  result = -1;
      }

      
      V _;
      auto ret = lookUp(k, _);
      if (_ != v) {
        std::cout << "k: " << k  << " v: " << _ << std::endl;
        throw std::runtime_error("wrong thing happend!!");
      }

      if (result >= 0) nKeys ++;
      return result;
    }

    return 0;
  }
  
  void checkIntegrity() {
    // pass
  }
  
  inline bool isMember(K k) {
    uint32_t bucketId = model(k) % buckets_.size();
    const Bucket &bucket = buckets_[bucketId];
    V _;

    if (FindInBucket(k, bucket, _)) {
      return true;
    }
    
    return false;
  }
  
  inline int remove(const K &k) {
    uint32_t bucketId = model(k) % buckets_.size();
    Bucket &bucket = buckets_[bucketId];
    if (RemoveInBucket(k, bucket)) {
      nKeys--;
      return 0;
    }
    return -1;
  }
  
  // slot: 2    bucket: 30
  inline int changeValue(const K &k, V val) {
    uint32_t bucketId = model(k) % buckets_.size();
    Bucket &bucket = buckets_[bucketId];
    
    for (char slot = 0; slot < kSlotsPerBucket; slot++) {
      if (!(bucket.occupiedMask & (1 << slot))) continue;
      if (k == bucket.keys[slot]) {
        bucket.values[slot] = val;
        return 0;
      }
    }
    return -1;
  }
  
  // Returns true if found.  Sets *out = value.
  inline bool lookUp(const K &k, V &out) const {
    uint32_t bucketId = model(k) % buckets_.size();
    const Bucket &bucket = buckets_[bucketId];

    if (FindInBucket2(k, bucket, out)) { 
      return true;
    } 
    // there is sth wrong
    return false;
  }
  
  
  // This function will not take memory on the heap into account because we don't know the memory layout of keys on heap
  uint64_t getMemoryCost() const {
    return num_buckets_ * sizeof(buckets_[0]);
  }
  
  // For the associative cuckoo table, check all of the slots in
  // the bucket to see if the key is present.
  inline bool FindInBucket(const K &k, const Bucket &bucket, V &out) const {
    for (char s = 0; s < kSlotsPerBucket; s++) {
      if ((bucket.occupiedMask & (1U << s)) && (bucket.keys[s] == k)) {
        out = bucket.values[s];
        return true;
      }
    }
    for (auto phead = bucket.head; phead != nullptr; phead = phead->next) {
      if (phead->bar > k) return true;
      h.setSeed(phead->seed);
      for (size_t i = 0; i < kSlotPerLinknode; i++) {
        if (phead->keys[i] == k) {
          out = phead->values[i];
          return true;
        } 
      }
    }
    // std::cout << "found nothing" << std::endl;
    return false;
  }
  
  inline bool FindInBucket2(const K &k, const Bucket &bucket, V &out) const {
    AbslFastHasherTuple<K> h;
    h.setSeed(bucket.seed);
    /*for (char s = 0; s < kSlotsPerBucket; s++) {
      if ((bucket.occupiedMask & (1U << s)) && (bucket.keys[s] == k)) {
        out = bucket.values[s];
        return true;
      }
    }*/
    
    out = bucket.values[h(k) >> 6];
    for (auto phead = bucket.head; phead != nullptr; phead = phead->next) {
      if (phead->bar > k) return true;
      h.setSeed(phead->seed);
      out = phead->values[h(k) >> 6];
    }
    // std::cout << "found nothing" << std::endl;
    return false;
  }
    
  // For the associative cuckoo table, check all of the slots in
  // the bucket to see if the key is present.
  inline bool RemoveInBucket(const K &k, Bucket &bucket) {
    //TODO:
    for (char i = 0; i < kSlotsPerBucket; i++) {
      if ((bucket.occupiedMask & (1U << i)) && bucket.keys[i] == k) {
        bucket.occupiedMask ^= (1U << i);
        return true;
      }
    }
    return false;
  }
  
  inline bool putItem(const K &k, const V &v, uint32_t b, uint8_t slot) {
  
    Bucket &bucket = buckets_[b];
    
    bucket.keys[slot] = k;
    bucket.values[slot] = v;
    bucket.occupiedMask |= 1U << slot;
    uint8_t toSlot[4]; // we dont have to use this
    uint8_t seed = updateSeed(b, toSlot, slot);
    return true;
  }
  
  uint8_t updateSeed(uint32_t bktIdx, uint8_t *dpSlotMove = 0, char slotWithNewKey = -1) {
    Bucket &bucket = buckets_[bktIdx];
    AbslFastHasherTuple<K> h, oldH;
    bool occupied[4];
    
    for (uint8_t seed = 0; seed < 255; ++seed) {
      *(uint32_t *) occupied = 0U;
      h.setSeed(seed);
      //std::cout << "seed: " << unsigned(seed) << std::endl;
      bool success = true;
      
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (bucket.occupiedMask & (1 << slot)) {
          
          uint8_t i = uint8_t(h(bucket.keys[slot]) >> 6);
          //std::cout << unsigned(slot) << "'s hash value is: " << std::bitset<8>(h(bucket.keys[slot])) << std::endl;
          if (occupied[i]) {
            success = false;
            break;
          } else { occupied[i] = true; }
        }
      }
      
      if (success) {
        Bucket cpBucket = buckets_[bktIdx]; //copy
        //oldH.setSeed(cpBucket.seed);

        bucket.occupiedMask = 0U;
        bucket.seed = seed;
        h.setSeed(seed);
        for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
          if (cpBucket.occupiedMask & (1 << slot)) {
            K moveKey = cpBucket.keys[slot];
            // uint8_t oldSlot = uint8_t(oldH(moveKey) >> 6);
            uint8_t toSlot = uint8_t(h(moveKey) >> 6);
            bucket.occupiedMask |= 1U << toSlot;
            bucket.keys[toSlot] = moveKey;
            bucket.values[toSlot] = cpBucket.values[slot];
          }
        }
        return seed;
      }
    }
    throw runtime_error("Cannot generate a proper hash seed within 255 tries, which is rare");
  }
  

  // Constants for BFS cuckoo path search:
  // The visited list must be maintained for all but the last level of search
  // in order to trace back the path. The BFS search has two roots
  // and each can go to a total depth (including the root) of 5.
  // The queue must be sized for 4 * \sum_{k=0...4}{(3*kSlotsPerBucket)^k}.
  // The visited queue, however, does not need to hold the deepest level,
  // and so it is sized 4 * \sum{k=0...3}{(3*kSlotsPerBucket)^k}
  static constexpr int calMaxQueueSize() {
    int result = 0;
    int term = 4;
    for (int i = 0; i < kMaxBFSPathLen; ++i) {
      result += term;
      term *= ((2 - 1) * kSlotsPerBucket);
    }
    return result;
  }
  
  static constexpr int calVisitedListSize() {
    int result = 0;
    int term = 4;
    for (int i = 0; i < kMaxBFSPathLen - 1; ++i) {
      result += term;
      term *= ((2 - 1) * kSlotsPerBucket);
    }
    return result;
  }
  
  static constexpr int kMaxQueueSize = calMaxQueueSize();
  static constexpr int kVisitedListSize = calVisitedListSize();
  

  void print_data_statistics() {
  
  }

  std::string name() {
    // std::string prefix = (ManualPrefetch ? "Prefetched" : "");
    std::string prefix;
    return prefix + "KapilParrotControlPlane<" + std::to_string(sizeof(K)) + ", " +
           std::to_string(sizeof(V));
  }

  size_t directory_byte_size() const {
    return 0;
  }

  size_t model_byte_size() const { 
    return 0; 
  }

  size_t byte_size() const { 
    return model_byte_size() + directory_byte_size(); 
  }

};


template<class K, class V, size_t OverAlloc, class Model = learned_hashing::MonotoneRMIHash<K, 1000000>,
            uint VL = sizeof(V) * 8>
class ParrotDataPlane : public ParrotCommon<K, V, VL> {
public:
  using ParrotCommon<K, V, VL>::kSlotsPerBucket;
  using ParrotCommon<K, V, VL>::kLoadFactor;
  using ParrotCommon<K, V, VL>::kMaxBFSPathLen;
  
  using ParrotCommon<K, V, VL>::VDMask;
  using ParrotCommon<K, V, VL>::ValueMask;

  using ParrotCommon<K, V, VL>::kSlotPerLinknode;
  using ParrotCommon<K, V, VL>::LocatorSeedLength;
  using ParrotCommon<K, V, VL>::num_buckets_;
  using ParrotCommon<K, V, VL>::h;
  using ParrotCommon<K, V, VL>::digestH;
  using ParrotCommon<K, V, VL>::Sentinel;
  using ParrotCommon<K, V, VL>::Sentinel32;
  using ParrotCommon<K, V, VL>::Sentinel32Mask;

  typedef Ludo_PathEntry<K> PathEntry;
  
  struct LinkNode {
    std::array<K, kSlotPerLinknode> keys;
    std::array<V, kSlotPerLinknode> values;
    // uint8_t fingerprints[kSlotPerLinknode];
    LinkNode* next = nullptr;
    LinkNode() {
      std::fill(keys.begin(), keys.end(), Sentinel);
    }
    void insert(const K& key, const V& value, support::Tape<LinkNode>& tape) {
      LinkNode* previous = this;
      for (LinkNode* current = previous; current != nullptr; current = current->next) {
        for (size_t i = 0; i < kSlotPerLinknode; i++) {
          const auto &k = current->keys[i];
          if (k == Sentinel) {
            current->keys[i] = key;
            current->values[i] = value;
            return;
          } else if (k == key) {
            current->values[i] = value;
            return;
          } else if (k > key) {
            LinkNode* qhead = new LinkNode();
            uint idx = 0;
            for (uint j = i; j < kSlotPerLinknode; j++, idx++) {
              qhead->keys[idx] = current->keys[j];
              qhead->values[idx] = current->values[j];
            }
            for (uint j = idx; j < kSlotPerLinknode; j++) {
              qhead->keys[j] = 0;
            }
            current->keys[i] = key; current->values[i] = value;
            for (uint j = i + 1; j < kSlotPerLinknode; j++) {
              current->keys[j] = 0;
            }
            qhead->next = current->next;
            current->next = qhead;
            return;
          }
        }
        previous = current;
      }
      previous->next = tape.alloc();
      previous->next->insert(key, value, tape);
      return;
    }
  };
  
  struct Bucket { // only as parameters and return values for easy access. the storage is compact.
    uint8_t seed;
    K bar = Sentinel;
    V values[kSlotsPerBucket];
    uint32_t offsett = Sentinel32;
    // /LinkNode *head = nullptr;

    bool operator==(const Bucket &other) const {
      if (seed != other.seed) return false;
      for (char s = 0; s < kSlotsPerBucket; s++) {
        if (values[s] != other.values[s]) return false;
      }
      return true;
    }
    
    bool operator!=(const Bucket &other) const {
      return !(*this == other);
    }

    //uint64_t localSize() {
    //  return 0;
    //}
  };
  
  Model model;
  uint32_t empty_num_bucket;
  std::vector<Bucket> buckets;
  std::vector<uint8_t> lock = vector<uint8_t>(8192, 0);
  std::unique_ptr<support::Tape<LinkNode>> tape;
  std::vector<LinkNode*> overflow;

  ParrotDataPlane():tape(std::make_unique<support::Tape<LinkNode>>()) {}
  
  explicit ParrotDataPlane(const ParrotControlPlane<K, V, OverAlloc, Model, VL> &cp) 
    : model(cp.model), tape(std::make_unique<support::Tape<LinkNode>>()) {

    num_buckets_ = cp.num_buckets_;
    resetMemory();

    std::queue<uint32_t> q = emptyBuckets(cp);
    std::cout << "empty num: " << q.size() << ", total num: " << num_buckets_ << std::endl;
    uint64_t num_chain_node = 0;
    for (uint32_t bktIdx = 0; bktIdx < num_buckets_; ++bktIdx) {
      const typename ParrotControlPlane<K, V, OverAlloc, Model, VL>::Bucket &cpBucket = cp.buckets_[bktIdx];
      
      Bucket &dpBucket = buckets[bktIdx];
      dpBucket.seed = cpBucket.seed;
      memset(dpBucket.values, 0, kSlotsPerBucket * sizeof(V));
      
      AbslFastHasherTuple<K> locateHash(cpBucket.seed);
      //uint nkeys(0);
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (cpBucket.occupiedMask & (1U << slot)) {
          const K &k = cpBucket.keys[slot];
          dpBucket.values[locateHash(k) >> 6] = cpBucket.values[slot];
        }
      }
      
      uint32_t len_chain = 0;
      for (auto phead = cpBucket.head; phead != nullptr; phead = phead->next, len_chain++) {
      }
      if (len_chain > 0 && (len_chain > q.size())){ //|| q.empty()) {
        num_chain_node += len_chain;
        //std::cout << "This is bucket: " << bktIdx << ", chain len: " << len_chain  <<", qlen: " << q.size()<< std::endl;
        LinkNode* qhead = nullptr;
        for (auto phead = cpBucket.head; phead != nullptr; phead = phead->next) {
          LinkNode* dhead = new LinkNode();
          std::vector<std::pair<K, V>> tmp_pairs;
          for (char slot = 0; slot < kSlotPerLinknode; ++slot) {
            if (phead->occupiedMask & (1U << slot)) {
              tmp_pairs.push_back(make_pair(phead->keys[slot], phead->values[slot]));
            }
          }
          std::sort(tmp_pairs.begin(), tmp_pairs.end());
          for (size_t pair_idx = 0; pair_idx < tmp_pairs.size(); pair_idx ++) {
            dhead->keys[pair_idx] = tmp_pairs[pair_idx].first;
            dhead->values[pair_idx] = tmp_pairs[pair_idx].second;
          }
          dhead->next = qhead;
          qhead = dhead;
        }
        overflow.push_back(qhead);
        dpBucket.offsett = (1UL << 31) + overflow.size() - 1;      // flag + offsett in overflow
        //std::cout << "current overflow size is: " << overflow.size() << std::endl;
        //if (true) {
        //  uint32_t res = 0;
        //  for (auto phead = overflow.back(); phead != nullptr; phead = phead->next, res++) {
        //  }
        //  std::cout << "res length of this chain is: " << res << std::endl;
        //}

      } else {
        uint32_t previous_offsett = Sentinel32;
        for (auto phead = cpBucket.head; phead != nullptr; phead = phead->next) {
          auto dpBktIdx = q.front();
          q.pop();
          Bucket &newDpBucket = buckets[dpBktIdx];
          newDpBucket.seed = phead->seed;
          newDpBucket.bar = phead->bar;
          locateHash.setSeed(phead->seed);
          for (char slot = 0; slot < kSlotPerLinknode; ++slot) {
            if (phead->occupiedMask & (1U << slot)) {
              const K &k = phead->keys[slot];
              newDpBucket.values[locateHash(k) >> 6] = phead->values[slot];
            }
          }
          newDpBucket.offsett = previous_offsett;
          previous_offsett = dpBktIdx;
        }
        dpBucket.offsett = previous_offsett;
      }
    }
    std::cout << "The size of overflow is: " << overflow.size() << std::endl;
    std::cout << "The number of overflow nodes: " << num_chain_node << std::endl;
    std::cout << "construction finished in data plane" << std::endl;
    uint64_t ppp = (num_chain_node*sizeof(LinkNode) + num_buckets_*sizeof(Bucket))/(1024*1024);
    std::cout << "The final size: " << ppp << std::endl;
    //throw std::runtime_error("stop .");
  }
 
  std::queue<uint32_t> emptyBuckets(const ParrotControlPlane<K, V, OverAlloc, Model, VL> &cp) {
    std::queue<uint32_t> q;
    for (uint32_t bktIdx = 0; bktIdx < num_buckets_; ++bktIdx) {
      const typename ParrotControlPlane<K, V, OverAlloc, Model, VL>::Bucket &cpBucket = cp.buckets_[bktIdx];
      //uint nkeys(0);
      if ((cpBucket.occupiedMask & (1U << kSlotPerLinknode - 1)) == 0U) {
        q.push(bktIdx);
      }
    }
    return q;
  }

  ~ParrotDataPlane(){
  }

  inline void resetMemory() {
    buckets.resize(num_buckets_);
  }
  
  inline V lookUp(const K &k) {
    V out;
    // if (!lookUp(k, out)) throw runtime_error("key does not exist");
    auto ret = lookUp(k, out);
    return ret;
  }
  
  // Returns true if found.  Sets *out = value.
  inline int lookUp(const K &k, V &out) {
    //auto start = std::chrono::high_resolution_clock::now(); 
    //std::cout << "k: " <<k;
    uint32_t bucketId = model(k) % num_buckets_;
    //auto stop = std::chrono::high_resolution_clock::now(); 
    //auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); 
    //std::cout<< std::endl << "Model latency is: "<< duration.count() << " nanoseconds" << std::endl;
    Bucket &bucket = buckets[bucketId];
    out = bucket.values[AbslFastHasherTuple<K>(bucket.seed)(k) >> 6];
    uint32_t offsett = bucket.offsett;
    while (offsett != Sentinel32) {
      if (offsett & (1UL << 31)) {
        //auto ppt = offsett & ((1UL << 31) - 1);
        //std::cout << "offsett is: " << ppt << std::endl;
        FindInLinkNode(k, out, offsett & Sentinel32Mask);
        return 1;
      } else {
        Bucket &bucket = buckets[offsett];
        if (bucket.bar > k) 
          return 1;
        out = bucket.values[AbslFastHasherTuple<K>(bucket.seed)(k) >> 6];
        offsett = bucket.offsett;
      }
    }
    return 0;
  }

  int inline FindInLinkNode (const K &k, V &out, uint32_t linkIdx) {
    for (auto phead = overflow[linkIdx]; phead != nullptr; phead = phead->next) {
      for (size_t i = 0; i < kSlotPerLinknode; i++) {
        if (phead->keys[i] > k) {
          return 1;
        } else if (phead->keys[i] == k) {
          out = phead->values[i];
          return 0;
        }
      }
    }
    return 0;
  }
  /*
  inline int insert(const K &k, const V &v) {
    uint32_t bucketId = model(k) % num_buckets_;
    Bucket &bucket = buckets[bucketId];
    return bucket.insert(k, v, *tape);
  }

  inline int remove(const K &k) {
    // with assumption that there is no alien key, so strong
    uint32_t bucketId = model(k) % num_buckets_;
    Bucket &bucket = buckets[bucketId];
    if (bucket.head == nullptr) {
      bucket.values[AbslFastHasherTuple<K>(bucket.seed)(k) >> 6] = 0;
      return 0;
    } else {
      for (auto phead = bucket.head; phead != nullptr; phead = phead->next) {
        for (size_t i = 0; i < kSlotPerLinknode; i++) {
          if (phead->keys[i] > k) {
            return 1;
          } else if (phead->keys[i] == k) {
            bucket.values[AbslFastHasherTuple<K>(bucket.seed)(k) >> 6] = 0;
            return 0;
          }
        }
      }
    }
    return 1;  // not fail, just waste time
  }

  inline int scan2(const K &low_key, const K &high_key) {
    //TODO: scan between low_key, and high_key, 
    //but you dont know how many records do you have
    return 0;
  }

  forceinline V* scan(const K &low_key, const size_t len = 300) {
    const uint32_t capacity = len + 2*(kSlotsPerBucket-1);
    V res[capacity];
    uint idx = 0;
    uint32_t bucketId = model(low_key) % num_buckets_;
    while (bucketId < num_buckets_ && idx < capacity) {
      Bucket &bucket = buckets[bucketId];
      for (uint i =0; i < kSlotsPerBucket && idx < capacity; i++) {
        if (bucket.values[i] == Sentinel) 
          continue;
        res[idx++] = bucket.values[i];
      }
      for (auto phead = bucket.head; phead != nullptr && idx < capacity; phead = phead->next) {
        for (size_t i = 0; i < kSlotPerLinknode && idx < capacity; i++) {
          if (phead->keys[i] < low_key)
            continue;
          res[idx++] = phead->values[i];
        }
      }
      bucketId ++;
    }
    //std::cout << "idx: " << idx << std::endl;
    return res;
  }
  */
  inline uint64_t getMemoryCost() {
    uint64_t res(0);
    uint64_t num_link_node(0);
    std::cout << "overflow size: " << overflow.size() << std::endl;
    for (uint32_t bktIdx = 0; bktIdx < buckets.size(); bktIdx ++) {
      res += sizeof(Bucket);
      Bucket &bucket = buckets[bktIdx];
      if ((bucket.offsett & (1UL << 31)) && (bucket.offsett != Sentinel32)) {
        uint32_t linkIdx = bucket.offsett & ((1UL << 31) - 1);
        for (auto &phead = overflow[linkIdx]; phead != nullptr; phead = phead->next) {
          num_link_node ++;
          res += sizeof(LinkNode);
        }
      }
    }
    res += model.byte_size();
    std::cout << "link node number is: " << num_link_node << std::endl;
    return res;
  }
};

}

