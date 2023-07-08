# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/yiliu/build/learned/_deps/hashing-src"
  "/home/yiliu/build/learned/_deps/hashing-build"
  "/home/yiliu/build/learned/_deps/hashing-subbuild/hashing-populate-prefix"
  "/home/yiliu/build/learned/_deps/hashing-subbuild/hashing-populate-prefix/tmp"
  "/home/yiliu/build/learned/_deps/hashing-subbuild/hashing-populate-prefix/src/hashing-populate-stamp"
  "/home/yiliu/build/learned/_deps/hashing-subbuild/hashing-populate-prefix/src"
  "/home/yiliu/build/learned/_deps/hashing-subbuild/hashing-populate-prefix/src/hashing-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/yiliu/build/learned/_deps/hashing-subbuild/hashing-populate-prefix/src/hashing-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/yiliu/build/learned/_deps/hashing-subbuild/hashing-populate-prefix/src/hashing-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
