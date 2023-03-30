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
    CompactTree() {
        root = new TrieNode{std::string(""), nullptr, nullptr};
        root->left = new TrieNode{std::string(BL-1, '0'), nullptr, nullptr};
        root->right = new TrieNode{std::string(BL-1, '1'), nullptr, nullptr};
    }
    
    explicit CompactTree(std::vector<Bar> bars) {
        root = new TrieNode{std::string(""), nullptr, nullptr};
        root->left = new TrieNode{std::string(BL-1, '0'), nullptr, nullptr};
        root->right = new TrieNode{std::string(BL-1, '1'), nullptr, nullptr};
        sort(bars.begin(), bars.end());
        for(uint32_t i = 0; i < bars.size(); i++) {
            int ret = insert(bars[i]);
            if (ret < 0) {
                std::cout << "Insert failed for bars: " << i << std::endl;
                break;
            }
        }
    }

    struct TrieNode {
        std::string str;
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
                TrieNode* branch = new TrieNode{head->str.substr(0, pos), nullptr, nullptr};
                if (bitpiece[pos] == '0') {
                    branch->left = new TrieNode{bitstr.substr(shift+1), nullptr, nullptr};
                    branch->right = head;
                    if (leftchild) parent->left = branch;
                    else parent->right = branch;
                } else {
                    branch->right = new TrieNode{bitstr.substr(shift+1), nullptr, nullptr};
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

    int lookup(Bar key) {
        std::bitset<BL> bits(key); 
        std::string bitstr = bits.to_string();
        std::string readbar = "";
        TrieNode* head = root;
        bool left = false;
        int shift = 0;
        while (head != nullptr) {
            if (head->str != "") {
                if (bitstr.substr(shift, head->str.length()) != head->str) {
                    left = (bitstr.substr(shift, head->str.length()) > head->str)? false:true;
                    break;
                }
                readbar += head->str;
                shift += head->str.length();
            }
            head = bitstr[shift] == '0'? head->left: head->right;
            readbar += bitstr[shift];
            shift ++;
        }
        //TODO:
        if (shift < BL) {
            if (left) {
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->left != nullptr) {
                        head = head->left;
                        readbar += '0';
                    } else {
                        head = head->right;
                        readbar += '1';
                    }
                } 
            } else {
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->right != nullptr) {
                        head = head->right;
                        readbar += '1';
                    } else {
                        head = head->left;
                        readbar += '0';
                    }
                }
            }
        }
        readbar.pop_back();
        bits = std::bitset<BL>(readbar);
        return bits.to_ulong();
    }

    int lookup(Bar key, std::string& out) {
        std::bitset<BL> bits(key); 
        std::string bitstr = bits.to_string();
        std::string readbar = "";
        TrieNode* head = root;
        bool left = false;
        int shift = 0;
        while (head != nullptr) {
            if (head->str != "") {
                if (bitstr.substr(shift, head->str.length()) != head->str) {
                    left = (bitstr.substr(shift, head->str.length()) > head->str)? false:true;
                    break;
                }
                readbar += head->str;
                shift += head->str.length();
            }
            head = bitstr[shift] == '0'? head->left: head->right;
            readbar += bitstr[shift];
            shift ++;
        }
        //TODO:
        if (shift < BL) {
            if (left) {
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->left != nullptr) {
                        head = head->left;
                        readbar += '0';
                        continue;
                    } 
                    head = head->right;
                    readbar += '1';
                } 
            } else {
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->right != nullptr) {
                        head = head->right;
                        readbar += '1';
                        continue;
                    }
                    head = head->left;
                    readbar += '0';
                }
            }
        }
        readbar.pop_back();
        out = std::bitset<BL>(readbar).to_string();
        if (left) return 1; //left one pos
        else return 0;
    }

    int lookup(Bar key, Bar& out) {
        std::bitset<BL> bits(key);
        std::string bitstr = bits.to_string();
        std::string readbar = "";
        TrieNode* parent = root;
        TrieNode* head = root;
        int howtoleave = 2; //0:left, 1:right, 2:none
        int shift = 0;
        //head = bitstr[shift] == '0'? head->left: head->right;
        //readbar += bitstr[shift++];
        while (head != nullptr) {
            if (head->str != "") {
                if (bitstr.substr(shift, head->str.length()) != head->str) {
                    howtoleave = (bitstr.substr(shift, head->str.length()) > head->str)? 1:0;
                    break;
                } else {
                    readbar += head->str;
                    shift += head->str.length();
                }
            }
            parent = head;
            head = bitstr[shift] == '0'? head->left: head->right;
            readbar += bitstr[shift];
            shift ++;
        }

        if (shift < BL) {
            if (howtoleave >= 2) {
                if (parent->left) {
                    head = parent->left;
                    howtoleave = 1; //right
                } else {
                    head = parent->right;
                    howtoleave = 0; //right
                }
            }
            switch(howtoleave) {
            case 0:
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->left != nullptr) {
                        head = head->left;
                        readbar += '0';
                        continue;
                    }
                    head = head->right;
                    readbar += '1';
                }
                break;
            case 1:
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->right != nullptr) {
                        head = head->right;
                        readbar += '1';
                        continue;
                    }
                    head = head->left;
                    readbar += '0';
                }
                break;
            }
        }

        /*while (head != nullptr) {
            readbar += head->str;
            std::cout << readbar << std::endl;
            if (head->right != nullptr) {
                head = head->right;
                readbar += '1';
                continue;
            }
            readbar += '0';
            head = head->left; 
        }

        if (shift < BL && head != nullptr) {
            if (left) {
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->left != nullptr) {
                        head = head->left;
                        readbar += '0';
                        continue;
                    } 
                    head = head->right;
                    readbar += '1';
                } 
            } else {
                while (head != nullptr) {
                    readbar += head->str;
                    if (head->right != nullptr) {
                        head = head->right;
                        readbar += '1';
                        continue;
                    }
                    head = head->left;
                    readbar += '0';
                }
            }
        } else if (shift < BL && head == nullptr) {

        }*/

        readbar.pop_back();
        std::cout << "key is: " << bitstr << std::endl;
        std::cout << "readerbar is: " << readbar << std::endl;
        out = std::bitset<BL>(readbar).to_ullong();
        //std::cout << "lookup out is: " << out << std::endl;
        if (howtoleave <= 0) 
            return 1;                                           //left one pos
        else 
            return 0;
    }

    int remove(Bar key) {
        return 0;
    }

    std::pair<Bar, Bar> scanPiece(Bar low_bound, Bar high_bound) {
        return std::make_pair(lookup(low_bound), lookup(high_bound));
    }

    int scanPieceStr(Bar low_bound, Bar high_bound, 
                     std::string& p1, std::string& p2, std::bitset<2>& bits) {
        bits[0] = lookup(low_bound, p1);
        bits[1] = lookup(high_bound, p2);
        return 0;
    }

    void prefixCut(TrieNode* head, int pos) {
        std::queue<TrieNode*> q;
        q.push(head);
        while(!q.empty()) {
            auto qtmp = q.front();
            q.pop();
            qtmp->str = qtmp->str.substr(pos+1);
            if (qtmp->left)
                q.push(qtmp->left);
            if (qtmp->right)
                q.push(qtmp->right);  
        }
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
