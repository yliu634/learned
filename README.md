
# Hashing Benchmarking

This repository has the source code for the implementation to have artifact evaluation for Parrot Hashing. 


### Prerequisites

wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | sudo apt-key add -
sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main'
sudo apt-get update
sudo apt -y install cmake g++ clang libboost-all-dev zstd python3-pip python3-dev
sudo apt -y install google-perftools libgoogle-perftools-dev cmake build-essential pkgconf
sudo apt -y install gdb libssl-dev tmux liblua5.3-dev libnuma-dev
sudo pip3 install certifi==2021.10.8 gitdb==4.0.9 GitPython==3.1.20 numpy==1.19.5 pandas==1.1.5 plotly==5.3.1 pretty-html-table==0.9.14 python-dateutil==2.8.2 pytz==2021.3 six==1.16.0 smmap==5.0.0 tenacity==8.0.1 typing-extensions==3.10.0.2
sudo pip3 install scikit-build

### Installation 

Run the following command: `git clone --recurse-submodules https://github.com/yliu634/learned.git`

then download all four datasets from SOSD and unzip, put them under datasets/

### Build & Run./scripts/setup.sh
./scripts/benchmark.sh
