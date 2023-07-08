#pragma once

#include <unordered_map>
#include <vector>

#include "ludotable.hpp"
#include "include/blob/compactrie.hpp"
/*
The size of each dataPiece is 1112
rebuild
rebuild
insert i: 0; key: 979672113
insert i: 100; key: 980673954
insert i: 200; key: 981082446
insert i: 300; key: 981680350
insert i: 400; key: 982514950
rebuild
insert i: 500; key: 982862055
insert i: 600; key: 983412369
insert i: 700; key: 983910698
insert i: 800; key: 984092448
rebuild
insert i: 900; key: 0
*/
namespace masters_thesis {

template<class K, class V, int KL = sizeof(K) * 8>
class MultiLudoTable {
  public: 
    filestore::CompactTree<K>* ct;
    std::vector<LudoTable<K,V>*> mcp;

    MultiLudoTable() {
        ct = new filestore::CompactTree<K>();
    }
    explicit MultiLudoTable(const std::vector<K>& bars) {
        ct = new filestore::CompactTree<K>(bars);
        barsNum = bars.size();
        mcp.resize(barsNum + 2);
        for (auto it = mcp.begin(); it != mcp.end(); ++it) {
            *it = new LudoTable<K, V>(1024); //wrong
        }
    }

    explicit MultiLudoTable(const std::vector<K>& bars, 
                            const std::vector<std::vector<K>>& dataPieces) {
        ct = new filestore::CompactTree<K>(bars);
        barsNum = bars.size();
        mcp.resize(barsNum + 2);
        auto it = mcp.begin();
        std::cout << "The size of each dataPiece is " << dataPieces.size() << std::endl;
        for (int i = 0; i < bars.size(); i++, it++) {
            *it = new LudoTable<K, V>(dataPieces[i], 1024);
        }
        std::cout << "There are total bars number is: " << barsNum << std::endl;
    }

    inline int insert(const K key, const V val) {
        uint32_t piece = ct->lookUp(key);
        mcp[piece]->insert(key, val);
        return 1;
    }

    inline int remove(K key) { 
        return 0;
    }

    inline int lookup(const K key, V& val) {
        uint32_t piece = ct->lookUp(key);
        mcp[piece]->lookUp(key, val);
        return 1;
    }

    inline int pieceScan(const K key, std::vector<V>& res) {
        uint32_t piece = ct->lookUp(key);
        mcp[piece]->dumpValue(res);
        return 1;
    }

    inline int pieceScanTest(const K key, std::vector<V>& res) {
        auto start = std::chrono::high_resolution_clock::now(); 
        uint32_t piece = ct->lookUp(key);
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); 
        std::cout<< std::endl << "Compact trie lookup latency: "<< duration.count()*1.00 << " nanoseconds" << std::endl;

        start = std::chrono::high_resolution_clock::now();
        mcp[piece]->dumpValue(res);
        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); 
        std::cout<< std::endl << "Lookup in subtable latency: "<< duration.count()*1.00 << " nanoseconds" << std::endl;

        return 1;
    }

    std::vector<V> scan(K low_bound, uint32_t len = 300) {
        return {};
    }

    ~MultiLudoTable() {
    }

    std::string name() {
        return "multiple ludo table";
    }

    void print_data_statistics() {
        //std::cout << "print_data_statistics().." << std::endl;
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

