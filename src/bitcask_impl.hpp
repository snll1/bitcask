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
#include <folly/MPMCQueue.h>

#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bitcask.hpp"
#include "key_dir.hpp"
#include "storage.hpp"
namespace bitcask {

struct KVQueueEntry {
    std::string key;
    std::string value;
    bool tombstone;
    std::promise<bool> flush_promise;
};

class BitCaskImpl {
   public:
    BitCaskImpl(const std::string& dir, const Params& params);
    ~BitCaskImpl();

    std::future<bool> put(const std::string& key, const std::string& value, bool tombstone = false);
    std::optional<std::string> get(const std::string& key) const;
    std::future<bool> remove(const std::string& key);

   private:
    void init();
    void create_new_data_file(bool init);
    void load_all_data_files();
    bool flush_data_records(std::vector<DataRecord>& batch);
    void flush_worker();
    void compact_worker();
    void compact();
    void compact_data_file(std::shared_ptr<DataFile> orig_data_file);

   private:
    std::string data_dir_;
    Params params_;
    folly::ConcurrentHashMap<uint64_t, std::shared_ptr<DataFile>> data_files_;
    std::atomic<uint64_t> last_file_id_ = 0;
    std::shared_ptr<DataFile> active_data_file_;
    KeyDir key_dir_;
    std::mutex file_mutex_;
    mutable std::shared_mutex io_mutex_;
    folly::MPMCQueue<std::shared_ptr<KVQueueEntry>> flush_queue_;
    std::atomic<bool> stop_{false};
    std::thread flush_thread_;
    std::thread compact_thread_;
};

}  // namespace bitcask