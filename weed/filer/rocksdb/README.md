# Prepare the compilation environment on linux
- sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
- sudo apt-get update -qq
- sudo apt-get install gcc-6 g++-6 libsnappy-dev zlib1g-dev libbz2-dev -qq
- export CXX="g++-6" CC="gcc-6"

- wget https://launchpad.net/ubuntu/+archive/primary/+files/libgflags2_2.0-1.1ubuntu1_amd64.deb
- sudo dpkg -i libgflags2_2.0-1.1ubuntu1_amd64.deb
- wget https://launchpad.net/ubuntu/+archive/primary/+files/libgflags-dev_2.0-1.1ubuntu1_amd64.deb
- sudo dpkg -i libgflags-dev_2.0-1.1ubuntu1_amd64.deb

# Prepare the compilation environment on mac os
```
brew install snappy
```

# install rocksdb:
```
 export ROCKSDB_HOME=/Users/chris/dev/rocksdb

 git clone https://github.com/facebook/rocksdb.git $ROCKSDB_HOME
 pushd $ROCKSDB_HOME
 make clean
 make install-static
 popd
```

# install gorocksdb

```
export CGO_CFLAGS="-I$ROCKSDB_HOME/include"
export CGO_LDFLAGS="-L$ROCKSDB_HOME -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"

go get github.com/tecbot/gorocksdb
```
# compile with rocksdb

```
cd ~/go/src/github.com/seaweedfs/seaweedfs/weed
go install -tags rocksdb
```
