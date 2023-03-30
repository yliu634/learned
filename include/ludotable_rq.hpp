#pragma once

#include <unordered_map>
#include <vector>

#include "ludotable.hpp"
#include "include/blob/compactrie.hpp"

namespace masters_thesis {

template<class K, class V, int KL = sizeof(K) * 8>
class MultiLudoTable {
  public: 
    filestore::CompactTree<K>* ct;
    std::unordered_map<K, typename std::list<LudoTable<K,V>*>::iterator> hashmap;
    std::list<LudoTable<K,V>*> mcp;

    MultiLudoTable() {
        ct = new filestore::CompactTree<K>();
    }
    explicit MultiLudoTable(const std::vector<K>& bars) {
        ct = new filestore::CompactTree<K>(bars);
        barsNum = bars.size();
        //bars constains "000000" as the first one.
        mcp.resize(barsNum);
        for (int i = 0; i < bars.size(); i++) {
            ct->insert(bars[i]);
        } 
        for (auto it = mcp.begin(); it != mcp.end(); ++it) {
            *it = new LudoTable<K, V>(1024); //init 1024
        }
    }

    explicit MultiLudoTable(const std::vector<K>& bars, const std::vector<std::vector<K>>& dataPieces) {
        ct = new filestore::CompactTree<K>(bars);
        barsNum = bars.size();
        mcp.resize(barsNum);
        auto it = mcp.begin();
        std::cout << "The size of each datapieces is " << dataPieces.size() << std::endl;
        for (int i = 0; i < bars.size(); i++, it++) {
            *it = new LudoTable<K, V>(dataPieces[i], 1024);
            //std::string str = std::bitset<KL>(bars[i]).to_string();
            hashmap[bars[i]] = it;
        }
        std::cout << "There are total bars number is: " << barsNum << std::endl;
    }

    inline int insert(const K key, const V val) {
        K piece;
        int ret = ct->lookup(key, piece);
        auto it = hashmap.find(piece);
        if (it != hashmap.end()) {
            (*(it->second))->insert(key, val);
        } else {
            throw std::runtime_error("cannot insert key into the ludo table.");
        }
    }

    inline int remove(K key) {}
    //int insert(K key, LudoTable* ludo) {
    //} 

    inline int lookuptest(const K key, V val) {
        return 1;
    }

    inline int lookup(const K key, V val) {
        // dont use it for single ludo for test
        K piece;
        ct->lookup(key, piece);
        auto it = hashmap.find(piece);
        if (it != hashmap.end()) {
            (*(it->second))->lookUp(key, val);
        } else {
            throw std::runtime_error("cannot lookup key in that table.");
        }
        return 1;
    }

    std::vector<V> scan(K low_bound, K high_bound) {
        std::string piece1, piece2; 
        int ret = ct->lookup(low_bound, piece1);
        auto lowTab = hashmap.find(piece1);
        if (ret > 0) lowTab --;
        ret = ct->lookup(high_bound, piece2);
        auto highTab = hashmap.find(piece2);
        if (ret > 0) highTab --;
        std::vector<V> v1, v2;
        //v1 = lowTab->dumpAll();
        //v2 = highTab->dumpAll();
        v1.insert(v1.end(), v2.begin(), v2.end());
        return v1;
    }

    ~MultiLudoTable() {
    }

    std::string name() {
        return "multiple ludo table";
    }

    void print_data_statistics() {
        std::cout << "print_data_statistics().." << std::endl;
    }

    int directory_byte_size() {
        return 0;
    }
    int byte_size() {
        return 0;
    }

  private:
    int barsNum;

};

}

