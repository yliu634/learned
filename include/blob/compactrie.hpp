#pragma once

#include <iostream>
#include <queue>
#include <vector>
#include <cstring>
#include <numeric>
#include <bitset>
#include <cassert>
#include <algorithm>

namespace filestore {
template<class Bar, int BL = sizeof(Bar) * 8>
class CompactTree {
  public:
    CompactTree(): nodesNum(0) {
        root = new TrieNode{std::string(""), -1, -1, nullptr, nullptr};
        root->left = new TrieNode{std::string(BL-1, '0'), -1, -1, nullptr, nullptr};
        root->right = new TrieNode{std::string(BL-1, '1'), -1, -1, nullptr, nullptr};
    }
    
    explicit CompactTree(std::vector<Bar> bars): nodesNum(0) {
        root = new TrieNode{std::string(""), -1, -1, nullptr, nullptr};
        root->left = new TrieNode{std::string(BL-1, '0'), -1, -1, nullptr, nullptr};
        root->right = new TrieNode{std::string(BL-1, '1'), -1, -1, nullptr, nullptr};
        sort(bars.begin(), bars.end());
        for(uint32_t i = 0; i < bars.size(); i++) {
            int ret = insert(bars[i]);
            if (ret < 0) {
                std::cout << "Insert failed for bars: " << i << std::endl;
                break;
            }
        }
        treeTraversal(root);
        treeNodesNum(root);
        std::cout << "There are total nodes size: " << nodesNum << std::endl;
    }

    struct TrieNode {
        std::string str;
        int32_t leftbar;
        int32_t rightbar;
        TrieNode* left;
        TrieNode* right;
    };
    
    int insert(Bar key) {
        std::bitset<BL> bits(key); 
        std::string bitstr = bits.to_string();
        int shift = 0;
        bool leftchild = true;
        TrieNode* parent = root;
        leftchild = bitstr[shift] == '0'? true:false;
        TrieNode* head = bitstr[shift++] == '0'? root->left:root->right;
        while (head != nullptr && shift < BL) {
            std::string bitpiece = bitstr.substr(shift, head->str.length());
            if (bitpiece == head->str || head->str == "") {
                shift += head->str.length();
            } else {
                auto res = std::mismatch(bitpiece.begin(), bitpiece.end(), head->str.begin());
                int pos = std::distance(bitpiece.begin(), res.first);
                shift += pos;
                TrieNode* branch = new TrieNode{head->str.substr(0, pos), -1, -1, nullptr, nullptr};
                if (bitpiece[pos] == '0') {
                    branch->left = new TrieNode{bitstr.substr(shift+1), -1, -1, nullptr, nullptr};
                    branch->right = head;
                    if (leftchild) parent->left = branch;
                    else parent->right = branch;
                } else {
                    branch->right = new TrieNode{bitstr.substr(shift+1), -1, -1, nullptr, nullptr};
                    branch->left = head;
                    if (leftchild) parent->left = branch;
                    else parent->right = branch;
                }
                head->str = head->str.substr(pos+1);
                return 1;
            }
            parent = head;
            if (shift < bitstr.length()) {
                leftchild = bitstr[shift] == '0'? true:false;
                head = bitstr[shift++] == '0'? head->left:head->right;
            }
        }
        if (shift >= BL) {
            std::cout << "Already in it" << std::endl;
        }
        return 0;
    }

    int32_t lookUp(Bar key) {
        std::bitset<BL> bits(key);
        std::string bitstr = bits.to_string();
        std::string readbar = "";
        TrieNode* parent = root;
        TrieNode* head = root;
        int shift = 0;
        while (head != nullptr) {
            if (head->str != "") {
                if (bitstr.substr(shift, head->str.length()) != head->str) {
                    if (bitstr.substr(shift, head->str.length()) > head->str) {
                        return head->rightbar;
                    } else {
                        return head->leftbar;
                    }
                }
                readbar += head->str;
                shift += head->str.length();
            }
            parent = head;
            head = bitstr[shift] == '0'? head->left: head->right;
            readbar += bitstr[shift];
            shift ++;
        }

        return parent->rightbar;
    }

    int remove(Bar key) {
        return 0;
    }

    std::pair<Bar, Bar> scan(Bar low_key, Bar high_key) {
        return {-1, -1};
    }

    int32_t piece = -1;
    void treeTraversal(TrieNode* head) {
        if (!head) return;
        head->leftbar = piece;
        if (head->left) 
            treeTraversal(head->left);
        if (!head->left && !head->right) 
            piece ++;
        if (head->right)
            treeTraversal(head->right);
        head->rightbar = piece;
    }

    void treeNodesNum(TrieNode* head) {
        if (!head) return;
        if (head->left) 
            treeNodesNum(head->left);
        if (head->right)
            treeNodesNum(head->right);
        nodesNum ++;
    }

    uint32_t statNodes() {
        return nodesNum;
    }

    void printTree() {
        TrieNode* head = root;
        std::queue<TrieNode*> q;
        q.push(head);
        while(!q.empty()) {
            auto qtmp = q.front();
            q.pop();
            if (qtmp->str.length() <= 0) {
                std::cout << "n/a" << std::endl;
            } else {
                std::cout << qtmp->str << std::endl;
            }
            if (qtmp->left)
                q.push(qtmp->left);
            if (qtmp->right)
                q.push(qtmp->right);  
        }
        return;
    }

    ~CompactTree() {
    }

  private:
    TrieNode* root;
    uint32_t nodesNum;
    

};
}

/*
int main() {
    std::vector<uint8_t> data = {1, 2, 240, 254}; 
    CompactTree<uint8_t> ct(data);
    //ct.printTree();
    auto res = ct.lookup(251);
    std::cout << res << std::endl;
    std::cout << "Pretty good" << std::endl;
    return 0;
}
*/
