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
#include <set>
#include <string>
#include <vector>

#include "bitcask_impl.hpp"
#include "key_dir.hpp"
#include "storage.hpp"
namespace fs = std::filesystem;

namespace bitcask {

BitCask::BitCask(const std::string& dir, const Params& params) : impl_(std::make_unique<BitCaskImpl>(dir, params)) {}
BitCask::~BitCask() { impl_.reset(); }

std::future<bool> BitCask::put(const std::string& key, const std::string& value) {
    return impl_->put(key, value);
}
std::optional<std::string> BitCask::get(const std::string& key) const {
    return impl_->get(key);
}
std::future<bool> BitCask::remove(const std::string& key) { return impl_->remove(key); }

BitCaskImpl::BitCaskImpl(const std::string& dir, const Params& params) : data_dir_(dir), params_(params), flush_queue_(65536) {
    init();
}

BitCaskImpl::~BitCaskImpl() {
    stop_ = truncate64;
    flush_thread_.join();
    if (params_.compaction_interval_secs) {
        compact_thread_.join();
    }
    std::cout << "~BitCaskImpl" << std::endl;
}

void BitCaskImpl::init() {
    if (!fs::exists(data_dir_)) {
        fs::create_directory(data_dir_);
        last_file_id_ = 0;
    } else {
        for (const auto& entry : fs::directory_iterator(data_dir_)) {
            if (entry.is_regular_file() && entry.path().extension() == ".data") {
                auto file_id = std::stoul(entry.path().stem().string());
                last_file_id_ = std::max(last_file_id_.load(), file_id);
                data_files_.insert(file_id, std::make_shared<DataFile>(entry.path().string(), file_id, false /* write */));
            }
        }
    }

    // Load all data files.
    load_all_data_files();

    // Create a new data file during init.
    create_new_data_file(true);

    // Start flush and compact thread
    flush_thread_ = std::thread(&BitCaskImpl::flush_worker, this);
    if (params_.compaction_interval_secs) {
        compact_thread_ = std::thread(&BitCaskImpl::compact_worker, this);
    }
}

std::future<bool> BitCaskImpl::put(const std::string& key, const std::string& value, bool tombstone) {
    auto record = std::make_shared<KVQueueEntry>(key, value, tombstone);
    flush_queue_.blockingWrite(record);
    return record->flush_promise.get_future();
}

void BitCaskImpl::flush_worker() {
    auto last_flush_time = std::chrono::high_resolution_clock::now();
    while (true) {
        if (stop_.load()) break;

        std::vector<DataRecord> batch;
        std::shared_ptr<KVQueueEntry> entry;
        std::vector<std::promise<bool>> promises;
        uint64_t size = 0;
        while (true) {
            if (stop_.load()) break;
            auto ret = flush_queue_.read(entry);
            if (ret) {
                batch.emplace_back(DataRecord{entry->key, entry->value, entry->tombstone});
                promises.emplace_back(std::move(entry->flush_promise));
                size += entry->key.size() + entry->value.size();
            }
            std::this_thread::sleep_for(std::chrono::microseconds(10));
            auto now = std::chrono::high_resolution_clock::now();
            if (size >= params_.flush_batch_size ||
                std::chrono::duration_cast<std::chrono::microseconds>(now - last_flush_time).count() >= params_.flush_interval_usecs) {
                break;
            }
        }

        if (!batch.empty()) {
            flush_data_records(batch);
            for (auto& promise : promises) {
                promise.set_value(true);
            }
        }
        last_flush_time = std::chrono::high_resolution_clock::now();
    }
    std::cout << "Flush thread exiting " << std::endl;
}

bool BitCaskImpl::flush_data_records(std::vector<DataRecord>& batch) {
    if (active_data_file_->size() > params_.max_data_file_size) {
        create_new_data_file(false /*init*/);
    }

    auto data_file = active_data_file_;
    if (!data_file->write_records(batch)) {
        return false;
    }

    std::shared_lock lock(io_mutex_);
    for (auto& record : batch) {
        if (record.tombstone) {
            key_dir_.remove(record.key);
        } else {
            KeyDirEntry entry{.file_id = data_file->id(), .value_size = record.value.size(), .value_offset = record.value_offset};
            key_dir_.insert(record.key, entry);
        }
    }

    return true;
}

std::optional<std::string> BitCaskImpl::get(const std::string& key) const {
    std::shared_lock lock(io_mutex_);
    auto ret = key_dir_.get(key);
    if (!ret) {
        return {};
    }
    auto& key_dir_entry = ret.value();
    const auto& data_file = data_files_.at(key_dir_entry.file_id);
    std::string buffer;
    buffer.resize(key_dir_entry.value_size);
    if (!data_file->read_exact(key_dir_entry.value_offset, reinterpret_cast<uint8_t*>(buffer.data()),
                               key_dir_entry.value_size)) {
        return {};
    }

    return buffer;
}

std::future<bool> BitCaskImpl::remove(const std::string& key) {
    std::shared_lock lock(io_mutex_);
    auto ret = key_dir_.get(key);
    if (!ret) {
        std::promise<bool> p;
        p.set_value(false);
        return p.get_future();
    }

    return put(key, {}, true /* tombstone */);
}

void BitCaskImpl::create_new_data_file(bool init) {
    std::lock_guard lock(file_mutex_);
    if (!init) {
        auto& data_file = data_files_.at(last_file_id_.load());
        if (data_file->size() < params_.max_data_file_size) {
            return;
        }
    }
    auto new_file_id = last_file_id_.load() + 1;
    fs::path path = fs::path(data_dir_) / std::format("{:09}.data", new_file_id);
    std::cout << "Create new data file " << new_file_id << std::endl;
    data_files_.insert(new_file_id, std::make_shared<DataFile>(path.string(), new_file_id, true /* write */));
    last_file_id_++;
    active_data_file_ = data_files_.at(last_file_id_);
}

void BitCaskImpl::load_all_data_files() {
    // Load all data files in order.
    std::set<uint64_t> file_ids;
    for (auto& [file_id, _] : data_files_) {
        file_ids.insert(file_id);
    }

    for (auto& file_id : file_ids) {
        auto& data_file = data_files_.at(file_id);
        auto callback = [this, file_id](const DataRecordHeader& header, const DataRecord& record) {
            KeyDirEntry entry{.file_id = file_id, .value_size = header.value_size, .value_offset = record.value_offset};
            key_dir_.insert(record.key, entry);
        };
        std::cout << "Loading data file " << file_id << std::endl;
        data_file->read_all_records(callback);
    }
}

void BitCaskImpl::compact_worker() {
    while (true) {
        if (stop_.load()) break;

        compact();
        std::this_thread::sleep_for(std::chrono::seconds(params_.compaction_interval_secs));
    }
}

void BitCaskImpl::compact() {
    auto last_file_id = last_file_id_.load();
    std::vector<std::shared_ptr<DataFile>> non_active_files;
    for (auto& [file_id, data_file] : data_files_) {
        if (file_id != last_file_id) {
            non_active_files.emplace_back(data_file);
        }
    }

    for (auto& data_file : non_active_files) {
        compact_data_file(data_file);
    }
}

void BitCaskImpl::compact_data_file(std::shared_ptr<DataFile> orig_data_file) {
    // Compact the original data file to new data file.
    std::vector<std::pair<std::string, KeyDirEntry>> new_key_entries;
    fs::path path = fs::path(data_dir_) / std::format("{:05}.data.tmp", orig_data_file->id());
    auto new_data_file = std::make_shared<DataFile>(path.string(), orig_data_file->id(), true /* write */);
    uint64_t record_count = 0;
    auto callback = [&](const DataRecordHeader& header, const DataRecord& record) {
        auto ret = key_dir_.get(record.key);
        if (!ret) {
            // Ignore if key not found in key dir
            return;
        }
        auto key_dir_entry = ret.value();
        if (orig_data_file->id() != key_dir_entry.file_id || record.value_offset != key_dir_entry.value_offset) {
            // Ignore if file id or offset doesnt match, it will be stale value.
            return;
        }

        std::vector<DataRecord> records{record};
        if (!new_data_file->write_records(records)) {
            throw std::runtime_error("Compaction failed");
        }

        record_count++;
        KeyDirEntry entry{.file_id = new_data_file->id(),
                          .value_size = record.value.size(),
                          .value_offset = records[0].value_offset};
        new_key_entries.emplace_back(record.key, std::move(entry));
    };

    auto ret = orig_data_file->read_all_records(callback);
    assert(ret);

    // Take exclusive lock to atomically update the keydir and new data file.
    std::unique_lock lock(io_mutex_);
    if (record_count == 0) {
        // Remove the data file if all the entries are stale.
        data_files_.erase(orig_data_file->id());
        orig_data_file.reset();
        new_data_file.reset();
        fs::remove(orig_data_file->name());
        fs::remove(new_data_file->name());
        return;
    }

    // Rename the new compact tmp data file to original data file.
    fs::rename(new_data_file->name(), orig_data_file->name());
    data_files_.insert_or_assign(new_data_file->id(), new_data_file);
    // Update the key entries with latest value offsets
    for (auto& [key, entry] : new_key_entries) {
        key_dir_.insert(key, entry);
    }
}

}  // namespace bitcask