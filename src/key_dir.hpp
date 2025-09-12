/**
The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>

#include <string>
namespace bitcask {

struct KeyDirEntry {
    uint64_t file_id;
    uint64_t value_size;
    uint64_t value_offset;
    uint64_t tstamp;
};

class KeyDir {
   public:
    KeyDir() = default;
    ~KeyDir() = default;

    void insert(const std::string& key, const KeyDirEntry& entry) {
        key_dir_.insert_or_assign(key, entry);
    }
    void remove(const std::string& key) {
        key_dir_.erase(key);
    }

    std::optional<KeyDirEntry> get(const std::string& key) const {
        auto iter = key_dir_.find(key);
        if (iter == key_dir_.end()) {
            return {};
        }
        return iter->second;
    }

   private:
    folly::ConcurrentHashMap<std::string, KeyDirEntry> key_dir_;
};
}  // namespace bitcask