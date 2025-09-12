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
#include <string>
#include <unordered_map>
#include <vector>

#include "bitcask.hpp"
#include "key_dir.hpp"
#include "storage.hpp"

namespace bitcask {

class BitCaskImpl {
   public:
    BitCaskImpl(const std::string& dir, const Params& params);
    ~BitCaskImpl();

    bool put(const std::string& key, const std::string& value, bool tombstone = false);
    std::optional<std::string> get(const std::string& key) const;
    bool remove(const std::string& key);

   private:
    void init();
    void create_new_data_file();
    void load_all_data_files();

   private:
    std::string data_dir_;
    Params params_;
    std::map<uint64_t, std::unique_ptr<DataFile>> data_files_;
    DataFile active_data_file_;
    uint64_t last_file_id_ = 0;
    KeyDir key_dir_;
    std::mutex file_mutex_;
};

}  // namespace bitcask