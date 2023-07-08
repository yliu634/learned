#!/bin/bash

# This script is used for running learned index repo on cloudlab platform 
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | sudo apt-key add -
sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main'
sudo apt-get update
sudo apt -y install cmake g++ clang libboost-all-dev zstd python3-pip python3-dev
sudo apt -y install google-perftools libgoogle-perftools-dev cmake build-essential pkgconf
sudo apt -y install gdb libssl-dev tmux liblua5.3-dev
sudo pip3 install certifi==2021.10.8 gitdb==4.0.9 GitPython==3.1.20 numpy==1.19.5 pandas==1.1.5 plotly==5.3.1 pretty-html-table==0.9.14 python-dateutil==2.8.2 pytz==2021.3 six==1.16.0 smmap==5.0.0 tenacity==8.0.1 typing-extensions==3.10.0.2

git clone --recurse-submodules https://github.com/yliu634/learned.git
cd learned

mkdir datasets
cd datasets
wget https://users.soe.ucsc.edu/~yliu634/files/wiki_ts_200M_uint64.zst
zstd -d wiki_ts_200M_uint64.zst
#wget https://dvn-cloud.s3.amazonaws.com/10.7910/DVN/JGVF9A/171c74110da-ce662431821a?response-content-disposition=attachment%3B%20filename%2A%3DUTF-8%27%27books_200M_uint64.zst&response-content-type=application%2Foctet-stream&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230426T175658Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=AKIAIEJ3NV7UYCSRJC7A%2F20230426%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=1f09be45e94ececcdf3d386998d4241c3e226c8668f5682caa9785570a99f1c1
#zstd -d books_200M_uint64.zst
#wget --no-check-certificate --no-proxy https://dvn-cloud.s3.amazonaws.com/10.7910/DVN/JGVF9A/17198cd6ce7-d366641a26e5?response-content-disposition=attachment%3B%20filename%2A%3DUTF-8%27%27fb_200M_uint64.zst&response-content-type=application%2Foctet-stream&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230426T175905Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=AKIAIEJ3NV7UYCSRJC7A%2F20230426%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=65be0b4281a4b1ea3592775eb087e181c001afa5fc4d410d715223a48650c7af
#zstd -d fb_200M_uint64.zst
cd ..

sudo ./scripts/setup.sh
sudo ./scripts/benchmark.sh










