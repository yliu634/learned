#pragma once

#include <unordered_map>
#include <vector>
#include <absl/hash/hash.h>

#include "ludotable.hpp"

namespace masters_thesis {
template<class K, class V>
struct SubLudoTable{
    uint32_t localDepth;
    LudoTable<K,V>* cp;
    DataPlaneLudo<K,V>* dp;
};

template<class K, class V, int KL = sizeof(K) * 8>
class LudoTable_Extd {
  public:
    std::vector<SubLudoTable<K, V>> tpl;  // {{local_depth, ludo}, {...}}
    std::vector<uint32_t> tableIndex;

    uint32_t globalDepth;
    uint32_t subtableNum;
    uint32_t subtableSize;
    bool ycsb = false;

    FastHasher64<K> dir;
    uint64_t seed = 899;
    K archor = 1051442326630982;
    bool debug = false;

    LudoTable_Extd(): globalDepth(2), subtableSize(1024) {
        //dir.setSeed(0x0889);
        subtableNum = 1UL << globalDepth;
        for (uint i = 0; i < subtableNum; i++) {
            tpl.push_back({globalDepth, new LudoTable<K,V>(subtableSize), nullptr});
        }
        for (uint i = 0; i < subtableNum; i++) {
            tableIndex.push_back(i);
        }
    }

    LudoTable_Extd(std::vector<std::pair<K, V>>& data): 
                    globalDepth(2), subtableSize(1800) {
        //dir.setSeed(0xa2781e0689);
        subtableNum = 1UL << globalDepth;
        for (uint i = 0; i < subtableNum; i++) {
            tableIndex.push_back(i);
        }
        for (uint i = 0; i < subtableNum; i++) {
            tpl.push_back({globalDepth, new LudoTable<K,V>(subtableSize), nullptr});
        }
        for (uint32_t i = 0; i < data.size(); i++) {
            insert(data[i].first, i);
        }
        insertCompleted();
    }

    LudoTable_Extd(std::vector<std::pair<K, V>>& data, 
                   uint32_t subtableSize, 
                   uint32_t globalDepth_ = 2): globalDepth(globalDepth_) {
        //dir.setSeed(0x0889);
        subtableNum = 1UL << globalDepth;
        for (uint i = 0; i < subtableNum; i++) {
            tableIndex.push_back(i);
        }
        for (uint i = 0; i < subtableNum; i++) {
            tpl.push_back({globalDepth, new LudoTable<K,V>(subtableSize), nullptr});
        }
        for (uint32_t i = 0; i < data.size(); i++) {
            insert(data[i].first, i);
        }
        insertCompleted();
    }

    int dataMigration(uint32_t h, uint32_t from, uint32_t dest, uint32_t newLocalDepth) {
        std::vector<std::pair<K,V>> dumpValue;
        auto mask = (1ULL << newLocalDepth) - 1;
        tpl[from].cp->dumpKVPair(dumpValue);
        //std::cout << "before data moving" << std::endl;
        uint32_t sum(0);
        for (uint i = 0; i < dumpValue.size(); i++) {
            auto newfp = absl::HashOf(dumpValue[i].first) & mask;
            if (newfp != h) {
                sum ++;
                tpl[from].cp->remove(dumpValue[i].first);
                tpl[dest].cp->insert(dumpValue[i].first, dumpValue[i].second);
            }
        }
        ////// for 
        // if(ycsb) tpl[dest].dp = new DataPlaneLudo<K, V>(*(tpl[dest].cp));
        //std::cout << "Size of dumpvalue is: " << dumpValue.size() << " and we move " << sum << std::endl;
        return 1;
    }

    int splitSingleTable(uint32_t h, uint32_t index) {
        //std::cout << "single table splitting start:" << std::endl;
        auto &tb = tpl[index];
        auto localDepth = tb.localDepth ++;
        //uint32_t newIndex = (index + (subtableNum/2));
        tpl.push_back({localDepth + 1, new LudoTable<K,V>(subtableSize), nullptr});
        h = (h&((1ULL<<localDepth)-1));
        auto newfp = h + (1ULL<<localDepth);
        tableIndex[newfp] = tpl.size() - 1;

        //tb.localDepth ++;
        //tpl[newIndex].localDepth ++;
        int ret = dataMigration(h, index, tpl.size()-1, tb.localDepth);
        if (ret < 0) {
            throw std::runtime_error("data migration errors during single table split");
        }
        return 1;
    } 

    int splitAllTable(uint32_t h, uint32_t index) {
        //std::cout << "All table splitting start:" << std::endl;
        uint32_t newGlobalDepth = globalDepth + 1;
        tpl[index].localDepth = newGlobalDepth;
        for (uint32_t i = 0; i < (1UL<<globalDepth); i++) {
            tableIndex.push_back(tableIndex[i]);
            if (i == h) {
                tpl.push_back({newGlobalDepth, new LudoTable<K,V>(subtableSize), nullptr});
                tableIndex.pop_back();
                tableIndex.push_back(tpl.size()-1);
                int ret = dataMigration(h, index, tpl.size()-1, newGlobalDepth);
                if (ret < 0) {
                    throw std::runtime_error("data migration error during all tables split.");
                }
            }
        }
        globalDepth = newGlobalDepth;
        subtableNum = 2* subtableNum;
        
        return 1;
    }

    int extendTable(uint32_t h, uint32_t index) {
        auto &tb = tpl[index];
        int res(0);
        //std::cout << std::endl;
        //std::cout << "split caused index: " << index << std::endl;
        if (tb.localDepth < globalDepth) {
            res = splitSingleTable(h, index);
        } else {
            res = splitAllTable(h, index);
        }

        //std::cout << "Size of directory: " << tableIndex.size() << "" << std::endl;
        //std::cout << "and they are: " << std::endl;
        //for (int i = 0; i < tableIndex.size(); i++) {
        //    std::cout << "the " << i << "th points to: " << tableIndex[i] 
        //        << ", whose local depth is: " << tpl[tableIndex[i]].localDepth << std::endl; 
        //}
        //if (tableIndex.size() >= 128)
        //    exit(0);

        return res;
    }

    // key: 1051442326630982

    int insert(const K key, const V val, bool online = false) {
        auto h = absl::HashOf(key) & ((1ULL << globalDepth) - 1);
        auto index = tableIndex[h];
        auto ret = tpl[index].cp->insert_ext(key, val);
        if (ret.status == -2) {
            // insert fail, and check local depth and global depth, split all or just itself.
            //std::cout << "start extend: with h and index are:" << h << " : " << index << std::endl;
            int res = extendTable(h, index);
            return res;
        }
        //////////////////////////
        //if (ycsb) tpl[index].dp = new DataPlaneLudo<K, V>(*(tpl[index].cp));
        return 1;
    }


    int insertCompleted() {
        for (uint32_t i = 0; i < tpl.size(); i ++) {
            auto &tp = tpl[i];
            tp.dp = new DataPlaneLudo<K, V>(*(tp.cp));
        }
        return 0;
    }

    bool lookUp(const K key, V& out) {
        auto h = absl::HashOf(key) & ((1ULL << globalDepth) - 1);
        auto ret = tpl[tableIndex[h]].dp->lookUp(key, out);
        //auto ret = tpl[tableIndex[h]].dp->lookUpExtend(key, out, hk);
        return ret;
    }

    int remove() {
        //TODO: 
        return 0;
    }

    int update() {
        //insert();//same to insert
        return 0;
    }

    uint64_t byte_size() {
        uint64_t sum(0);
        for (uint i = 0; i < tpl.size(); i++) {
            sum += tpl[i].dp->getMemoryCost();
            // std::cout << "this dp size is: " << tpl[i].dp->getMemoryCost() << std::endl;
        }
        std::cout << "There are " << tpl.size() << " subtables in total" 
                << "and each of them: " << (tpl[0].dp->getMemoryCost())/1024.0 << "MB" << std::endl;
        sum += (tableIndex.size() * sizeof(uint32_t));
        return sum;
    }

    std::string name() {
        return "Ludo Extendible table memory cost: " + std::to_string(byte_size()) + " Bytes";
    }

};

}