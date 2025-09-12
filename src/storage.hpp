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

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <format>
#include <fstream>
#include <future>
#include <iostream>
#include <shared_mutex>

namespace bitcask {

struct DataRecordHeader {
    uint32_t crc{0};
    uint64_t timestamp{0};
    uint32_t key_size{0};
    uint32_t value_size{0};
    bool tombstone{false};
    // Key and value bytes are stored after this.
};

struct DataRecord {
    std::string key;
    std::string value;
    bool tombstone;
    uint64_t value_offset;
};

class DataFile {
   public:
    DataFile() = default;
    DataFile(const std::string &file, uint64_t file_id, bool write) : file_(file), file_id_(file_id), write_(write) {
        if (write_) {
            write_fd_ = open(file_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (write_fd_ == -1) {
                perror("");
                throw std::runtime_error("Failed to open file for write");
            }
        }
        read_fd_ = open(file_.c_str(), O_RDONLY);
        if (read_fd_ == -1) {
            perror("");
            throw std::runtime_error("Failed to open for read");
        }
    }

    ~DataFile() {
        if (write_fd_ != -1) close(write_fd_);
        if (read_fd_ != -1) close(read_fd_);
    }

    bool write_records(std::vector<DataRecord> &records) {
        uint64_t total_size = 0;
        for (auto &record : records) {
            total_size += sizeof(DataRecordHeader) + record.key.size() + record.value.size();
        }

        uint64_t file_offset = lseek(write_fd_, 0, SEEK_CUR);
        if (file_offset == -1) {
            perror("lseek failed");
            return false;
        }
        auto buffer = std::make_unique<uint8_t[]>(total_size);
        auto buffer_ptr = buffer.get();
        for (auto &record : records) {
            auto &key = record.key;
            auto &value = record.value;
            record.value_offset = file_offset + sizeof(DataRecordHeader) + key.size();
            auto header = reinterpret_cast<DataRecordHeader *>(buffer_ptr);
            header->crc = 0;
            header->timestamp = 0;
            header->key_size = key.size();
            header->value_size = value.size();
            header->tombstone = record.tombstone;
            buffer_ptr += sizeof(DataRecordHeader);
            std::memcpy(static_cast<uint8_t *>(buffer_ptr), key.data(), key.size());

            if (!value.empty()) {
                buffer_ptr += key.size();
                std::memcpy(static_cast<uint8_t *>(buffer_ptr), value.data(), value.size());
            }

            uint64_t record_size = sizeof(DataRecordHeader) + key.size() + value.size();
            buffer_ptr += record_size;
            file_offset += record_size;
        }
        return write_exact(buffer.get(), total_size);
    }

    bool write_exact(uint8_t *buffer, uint64_t size) {
        uint64_t total_written = 0;
        while (total_written < size) {
            ssize_t bytes_written = write(write_fd_, buffer + total_written, size - total_written);
            if (bytes_written == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                perror("write failed");
                return false;
            } else if (bytes_written == 0) {
                break;
            }
            total_written += bytes_written;
        }

        return total_written == size ? true : false;
    }

    bool read_all_records(std::function<void(const DataRecordHeader &, const DataRecord &)> callback) {
        uint64_t offset = 0;
        while (true) {
            DataRecordHeader header;
            // Read the header
            auto ret = read_exact(offset, reinterpret_cast<uint8_t *>(&header), sizeof(header));
            if (ret == -1) {
                return false;
            } else if (ret == 0) {
                break;
            }

            // Read the whole key value
            offset += sizeof(header);
            uint64_t kv_size = header.key_size + header.value_size;
            auto kv_buffer = std::make_unique<uint8_t[]>(kv_size);
            ret = read_exact(offset, kv_buffer.get(), kv_size);
            if (ret == -1) {
                return false;
            }

            std::string tkey;
            tkey.assign(reinterpret_cast<char *>(kv_buffer.get()), header.key_size);
            offset += header.key_size;
            std::string tvalue;
            tvalue.assign(reinterpret_cast<char *>(kv_buffer.get()), header.value_size);
            uint64_t value_offset = offset;
            offset += header.value_size;

            DataRecord record{.key = tkey, .value = tvalue, .value_offset = value_offset};
            callback(header, record);
        }
        return true;
    }

    uint64_t read_exact(uint64_t offset, uint8_t *buffer, uint64_t size) const {
        int total_read = 0;
        while (total_read < size) {
            ssize_t bytes_read = pread(read_fd_, buffer + total_read, size - total_read, offset);
            if (bytes_read == 0) {
                break;
            } else if (bytes_read == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                perror("read failed");
                return -1;
            }

            total_read += bytes_read;
        }

        return total_read;
    }

    uint64_t size() {
        struct stat st;
        if (fstat(write_fd_, &st) != 0) {
            perror("fstat error");
            return 0;
        }
        return st.st_size;
    }

    uint64_t id() { return file_id_; }
    std::string name() { return file_; }

    void inc_dead_records() {
        dead_records_++;
        num_records_++;
    }

    void inc_num_records() {
        num_records_++;
    }

    double dead_record_ratio() {
        return (double)dead_records_ / num_records_;
    }

   private:
    uint64_t num_records_ = 0;
    uint64_t dead_records_ = 0;
    std::string file_;
    int32_t write_fd_ = -1;
    int32_t read_fd_ = -1;
    uint64_t file_id_;
    bool write_ = false;
};

}  // namespace bitcask