#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <random>

using namespace std;

class Storage {
public:
   
  int storageFile = -1; // fd
  uint32_t size;
  string fileName;
  size_t blockSize;
  // char* data = new char[size];

  Storage(string fileName, uint32_t size, size_t blockSize = 4096, uint16_t port = 0) :
      size(size), blockSize(blockSize), fileName(fileName) {
    storageFile = open(fileName.c_str(), O_RDWR | O_CREAT | O_SYNC | O_DIRECT, 0666);
    ftruncate(storageFile, size * blockSize);

    numLock = 0;
    //rd_ = new random_device();
    //gen_ = new mt19937(rd_());
    //char* data = new char[blockSize];
    buff_ = vector<char>(blockSize);
    //dis_ = new uniform_int_distribution<>(0, 255);

    /*for (std::size_t i = 0; i < size; ++i) {
            data[i] = static_cast<char>(dis(gen_));
    }*/
  }
  
  ~Storage() {
    stop();
  }
  
  void stop() {
    close(storageFile);
  }
  
  recursive_mutex locks[8192];
  
  int stocInsert(uint64_t pos, vector<char> & value) {
    mylock_guard g(locks[(numLock ++) % 8192]);
    //addrChange(pos);
    pwrite(storageFile, value.data(), blockSize, pos * blockSize);
    
  }
  
  int stocUpdate(uint64_t pos, vector<char> & value) {
    stocInsert(pos, value);
  }

  int stocRead(uint64_t pos, vector<char> & value) {
    // addrChange(pos);
    // vector<char> buff(blockSize);
    mylock_guard g(locks[(numLock ++) % 8192]);
    pread(storageFile, value.data(), blockSize, pos * blockSize);

  }

private:
  uint32_t numLock;
  std::random_device rd_;
  std::mt19937 gen_;
  vector<char> buff_;
  std::uniform_int_distribution<> dis_;

 
};
