# Bitcask

A thread safe bitcask key value store in C++20

## Features
* Concurrent safe put, get and erase. Uses folly concurrent hashmaps and queues.
* Fine tune the background batch flushing.
* Background compaction thread.


## Usage
```c++
#include <bitcask/bitcask.h>

int main() {
    auto bc = BitCask("bitcask_dir", Params{});
    bc.put("hello", "world").get();
    auto value = bc.get("hello").value();
    bc.remove("hello").get();
}
```

## Build
### Dependencies :
* folly
* gflags
* glog

```#!bash

$ mkdir build
$ cd build
$ cmake .. -DCMAKE_BUILD_TYPE=Debug -DFolly_DIR=<folly directory>
$ make
$ make test

```

## License
[MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)