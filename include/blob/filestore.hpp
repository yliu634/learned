#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <random>
#include <algorithm>

using namespace std;

namespace filestore {
class Storage {
public:
   
  int storageFile = -1; // fd
  uint32_t size;
  string fileName;
  size_t blockSize;
  recursive_mutex locks[8192];
  // char* data = new char[size];

  explicit Storage(string fileName, uint32_t size, size_t blockSize = 4096, uint16_t port = 0) :
      size(size), blockSize(blockSize), fileName(fileName) {

    size_t num_bytes = size * blockSize;
    std::ofstream ofs(fileName, std::ios::out|std::ios::binary);
    if (!ofs) {
      std::cerr << "Error: failed to open file \"" << fileName << "\"\n";
      return;
    }
    std::vector<char> buffer(1024 * 1024, '\0'); // 1 MB buffer
    while (num_bytes > 0) {
        std::size_t bytes_to_write = std::min(buffer.size(), num_bytes);
        ofs.write(buffer.data(), bytes_to_write);
        num_bytes -= bytes_to_write;
        if (!ofs) {
          std::cerr << "Error: could not write to file '" << fileName << "'\n";
          return;
        }
    }
    ofs.close();
    
    //boost::filesystem::path filePath(fileName);
    //std::uintmax_t fileSize = boost::filesystem::file_size(filePath);
    //std::cout << "After writing file size: " << fileSize << " bytes" << std::endl;

    storageFile = open(fileName.c_str(), O_RDWR | O_CREAT | O_SYNC | O_DIRECT, 0666);
    ftruncate(storageFile, size * blockSize);
    
    numLock = 0;
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
  
  int stocInsert(uint64_t pos, vector<char> & value) {
    mylock_guard g(locks[(numLock ++) % 8192]);
    //addrChange(pos);
    pwrite(storageFile, value.data(), blockSize, pos * blockSize);
    return 1;
  }
  
  int stocUpdate(uint64_t pos, vector<char> & value) {
    stocInsert(pos, value);
    return 1;
  }

  int stocRead(uint64_t pos, vector<char> & value) {
    // addrChange(pos);
    // vector<char> buff(blockSize);
    mylock_guard g(locks[(numLock ++) % 8192]);
    pread(storageFile, value.data(), blockSize, pos * blockSize);
    return 1;
  }

private:
  uint32_t numLock;
  std::random_device rd_;
  std::mt19937 gen_;
  vector<char> buff_;
  std::uniform_int_distribution<> dis_;

 
};
}

