#pragma once

#define _GNU_SOURCE

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
  uint32_t disktime = 0;

  explicit Storage() {

  }

  explicit Storage(string fileName, uint32_t size, size_t blockSize = 512, uint16_t port = 0) :
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
    wbuf = new char[blockSize];
    //dis_ = new uniform_int_distribution<>(0, 255);

  }
  
  ~Storage() {
    stop();
  }
  
  void stop() {
    close(storageFile);
  }
  
  int stocInsert(uint64_t pos, vector<char> & value) {
    std::lock_guard g(locks[(numLock ++) % 8192]);
    std::memcpy(wbuf, value.data(), blockSize);
    posix_memalign(&wbuf, blockSize, blockSize);
    int nwrite = pwrite(storageFile, wbuf, blockSize, pos * blockSize);
    if (nwrite < 0) {
      std::cout << "There is sth wrong with stoc.write()" << std::endl;
    }
    return nwrite;
  }
  
  int stocUpdate(uint64_t pos, vector<char> & value) {
    stocInsert(pos, value);
    return pos;
  }

  int stocRead(uint64_t pos, vector<char> &value) {
    // vector<char> buff(blockSize);
    // std::lock_guard g(locks[(numLock ++) % 8192]);
    disktime ++;
    void *buf;
    posix_memalign(&buf, blockSize, blockSize);
    int nread = pread(storageFile, buf, blockSize, pos * blockSize);
    // std::cout << "ludo read disk: " << disktime << std::endl;
    return nread;
  }

private:
  uint32_t numLock;
  void *wbuf;
  std::random_device rd_;
  std::mt19937 gen_;
  vector<char> buff_;
  std::uniform_int_distribution<> dis_;

 
};
}

