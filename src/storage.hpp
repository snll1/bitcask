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
#include <shared_mutex>

namespace bitcask {

struct DataRecord {
    uint32_t crc{0};
    uint64_t timestamp{0};
    uint32_t key_size{0};
    uint32_t value_size{0};
    bool tombstone{false};
    // Key and value bytes are stored after this.
};

class DataFile {
   public:
    DataFile() = default;
    DataFile(const std::string &file, bool active) : file_(file), active_(active) {
        if (active) {
            write_fd_ = open(file_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        }
        read_fd_ = open(file_.c_str(), O_RDONLY);
    }

    ~DataFile() {
        if (write_fd_ != -1) close(write_fd_);
        if (read_fd_ != -1) close(read_fd_);
    }

    bool write_record(const std::string &key, const std::string &value, uint64_t &value_offset,
                      bool tombstone = false) {
        value_offset = lseek(write_fd_, 0, SEEK_CUR);
        if (value_offset == -1) {
            perror("lseek failed");
            return false;
        }
        value_offset += sizeof(DataRecord) + key.size();

        uint64_t total_size = sizeof(DataRecord) + key.size() + value.size();
        auto buffer = std::make_unique<uint8_t[]>(total_size);
        auto record = reinterpret_cast<DataRecord *>(buffer.get());
        record->crc = 0;
        record->timestamp = 0;
        record->key_size = key.size();
        record->value_size = value.size();
        record->tombstone = tombstone;
        auto ptr = buffer.get();
        ptr += sizeof(DataRecord);
        std::memcpy(static_cast<uint8_t *>(ptr), key.data(), key.size());

        if (!value.empty()) {
            ptr += key.size();
            std::memcpy(static_cast<uint8_t *>(ptr), value.data(), value.size());
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

    bool read_all_records(std::function<void(const std::string &, uint64_t, uint64_t)> callback) {
        uint64_t offset = 0;
        while (true) {
            DataRecord record;
            // Read the record header
            auto ret = read_exact(offset, reinterpret_cast<uint8_t *>(&record), sizeof(record));
            if (ret == -1) {
                return false;
            } else if (ret == 0) {
                break;
            }

            // Read the whole key value
            uint64_t key_size = record.key_size;

            auto key_buffer = std::make_unique<uint8_t[]>(key_size);
            offset += sizeof(record);
            ret = read_exact(offset, key_buffer.get(), key_size);
            if (ret == -1) {
                return false;
            }

            std::string key;
            key.assign(reinterpret_cast<char *>(key_buffer.get()), record.key_size);

            callback(key, record.value_size, offset + record.key_size);
            offset += record.key_size + record.value_size;
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
        assert(fstat(write_fd_, &st) == 0);
        return st.st_size;
    }

   private:
    std::string file_;
    int32_t write_fd_ = -1;
    int32_t read_fd_ = -1;
    bool active_ = false;
};

}  // namespace bitcask