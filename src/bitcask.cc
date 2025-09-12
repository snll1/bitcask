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

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "bitcask_impl.hpp"
#include "key_dir.hpp"
#include "storage.hpp"
namespace fs = std::filesystem;

namespace bitcask {

BitCask::BitCask(const std::string& dir, const Params& params) : impl_(std::make_unique<BitCaskImpl>(dir, params)) {}
BitCask::~BitCask() {}

bool BitCask::put(const std::string& key, const std::string& value) {
    return impl_->put(key, value);
}
std::optional<std::string> BitCask::get(const std::string& key) const {
    return impl_->get(key);
}
bool BitCask::remove(const std::string& key) { return impl_->remove(key); }

BitCaskImpl::BitCaskImpl(const std::string& dir, const Params& params) : data_dir_(dir), params_(params) {
    init();
}

BitCaskImpl::~BitCaskImpl() {}

void BitCaskImpl::init() {
    if (!fs::exists(data_dir_)) {
        fs::create_directory(data_dir_);
        last_file_id_ = 0;
    } else {
        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            if (entry.is_regular_file() && entry.path().extension() == ".data") {
                auto file_id = std::stoul(entry.path().stem().string());
                last_file_id_ = std::max(last_file_id_, file_id);
                data_files_[file_id] = std::make_unique<DataFile>(entry.path().string(), false /* active */);
            }
        }
    }

    // Load all data files.
    load_all_data_files();

    // Create a new data file during init.
    create_new_data_file();
}

bool BitCaskImpl::put(const std::string& key, const std::string& value, bool tombstone) {
    {
        auto& data_file = data_files_.at(last_file_id_);
        if (data_file->size() > params_.max_data_file_size) {
            create_new_data_file();
        }
    }

    uint64_t value_offset = 0;
    auto& data_file = data_files_.at(last_file_id_);
    if (!data_file->write_record(key, value, value_offset)) {
        return false;
    }

    if (tombstone) {
        key_dir_.remove(key);
    } else {
        KeyEntry entry{.file_id = last_file_id_, .value_size = value.size(), .value_offset = value_offset};
        key_dir_.insert(key, entry);
    }
    return true;
}

std::optional<std::string> BitCaskImpl::get(const std::string& key) const {
    auto key_entry = key_dir_.get(key);
    auto& data_file = data_files_.at(key_entry.file_id);
    std::string buffer;
    buffer.resize(key_entry.value_size);
    if (!data_file->read_exact(key_entry.value_offset, reinterpret_cast<uint8_t*>(buffer.data()),
                               key_entry.value_size)) {
        return {};
    }

    return buffer;
}

bool BitCaskImpl::remove(const std::string& key) {
    return put(key, {}, true /* tombstone */);
}

void BitCaskImpl::create_new_data_file() {
    std::lock_guard lock(file_mutex_);
    auto& data_file = data_files_.at(last_file_id_);
    if (data_file->size() < params_.max_data_file_size) {
        return;
    }
    last_file_id_++;
    fs::path path = fs::path(data_dir_) / std::format("{:05}.data", last_file_id_);
    std::cout << "Create new data file " << last_file_id_ << std::endl;
    data_files_[last_file_id_] = std::make_unique<DataFile>(path.string(), true /* active */);
}

void BitCaskImpl::load_all_data_files() {
    // Load all data files in order.
    for (auto& [file_id, data_file] : data_files_) {
        auto callback = [this, file_id](const std::string& key, uint64_t value_size, uint64_t value_offset) {
            KeyEntry entry{.file_id = file_id, .value_size = value_size, .value_offset = value_offset};
            key_dir_.insert(key, entry);
        };
        std::cout << "Loading data file " << file_id << std::endl;
        data_file->read_all_records(callback);
    }
}

}  // namespace bitcask