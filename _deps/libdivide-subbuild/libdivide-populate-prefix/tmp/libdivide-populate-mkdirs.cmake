# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/yiliu/build/learned/_deps/libdivide-src"
  "/home/yiliu/build/learned/_deps/libdivide-build"
  "/home/yiliu/build/learned/_deps/libdivide-subbuild/libdivide-populate-prefix"
  "/home/yiliu/build/learned/_deps/libdivide-subbuild/libdivide-populate-prefix/tmp"
  "/home/yiliu/build/learned/_deps/libdivide-subbuild/libdivide-populate-prefix/src/libdivide-populate-stamp"
  "/home/yiliu/build/learned/_deps/libdivide-subbuild/libdivide-populate-prefix/src"
  "/home/yiliu/build/learned/_deps/libdivide-subbuild/libdivide-populate-prefix/src/libdivide-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/yiliu/build/learned/_deps/libdivide-subbuild/libdivide-populate-prefix/src/libdivide-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/yiliu/build/learned/_deps/libdivide-subbuild/libdivide-populate-prefix/src/libdivide-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
