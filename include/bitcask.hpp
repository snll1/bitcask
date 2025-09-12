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
#include <future>
#include <memory>
#include <optional>
#include <string>

namespace bitcask {

struct Params {
    uint64_t max_data_file_size = 512 * 1024 * 1024;
    uint64_t compaction_interval_secs = 0;
    uint64_t flush_batch_size = 8 * 1024 * 1024;
    uint64_t flush_interval_usecs = 50;
    bool fsync_mode = false;
    double compact_dead_ratio = 0.4;
    double merge_min_data_file_ratio = 0.3;
};

class BitCaskImpl;

class BitCask {
   public:
    BitCask(const std::string& dir, const Params& params);
    ~BitCask();

    std::future<bool> put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    std::future<bool> remove(const std::string& key);

   private:
    std::unique_ptr<BitCaskImpl> impl_;
};
}  // namespace bitcask
