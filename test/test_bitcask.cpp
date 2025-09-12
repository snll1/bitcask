
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <bitcask.hpp>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>

using namespace std;
using namespace bitcask;
DEFINE_int32(num_kvs, 1000, "Number of kvs used for testing.");
const char* test_dir_ = "/tmp/bc_test_dir";
class BitCaskTest : public ::testing::Test {
   public:
    BitCaskTest() : rng_(chrono::high_resolution_clock::now().time_since_epoch().count()), char_dist_(32, 126) {}
    void SetUp() override {}
    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    string generate_random_string(int start, int end) {
        uniform_int_distribution<int> length_dist(start, end);
        int length = length_dist(rng_);
        string random_string(length, ' ');
        for (int i = 0; i < length; ++i) {
            random_string[i] = static_cast<char>(char_dist_(rng_));
        }

        return random_string;
    }

    string random_key() { return generate_random_string(16, 128); }
    string random_value() { return generate_random_string(128, 1024); }
    vector<pair<string, string>> generate_random_kvs(int count) {
        vector<pair<string, string>> kvs;
        kvs.reserve(count);
        for (int i = 0; i < count; i++) {
            kvs.emplace_back(random_key(), random_value());
        }
        return kvs;
    }

   private:
    mt19937_64 rng_;
    uniform_int_distribution<int> char_dist_;
};

TEST_F(BitCaskTest, put_test) {
    int count = FLAGS_num_kvs;
    auto kvs = generate_random_kvs(count);
    {
        BitCask bc(test_dir_, Params{});
        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.put(key, value).get(), true);
        }

        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.get(key).value(), value);
        }
    }

    {
        BitCask bc(test_dir_, Params{});
        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.get(key).value(), value);
        }
    }
}

TEST_F(BitCaskTest, update_test) {
    int count = FLAGS_num_kvs;
    auto kvs = generate_random_kvs(count);
    {
        BitCask bc(test_dir_, Params{});
        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.put(key, value).get(), true);
        }

        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.get(key).value(), value);
        }

        // Update the key with new random values.
        for (auto& [key, value] : kvs) {
            value = random_value();
        }

        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.put(key, value).get(), true);
        }

        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.get(key).value(), value);
        }
    }

    {
        BitCask bc(test_dir_, Params{});
        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.get(key).value(), value);
        }
    }
}

TEST_F(BitCaskTest, remove_test) {
    int count = FLAGS_num_kvs;
    auto kvs = generate_random_kvs(count);
    {
        BitCask bc(test_dir_, Params{});
        for (auto& [key, value] : kvs) {
            ASSERT_EQ(bc.put(key, value).get(), true);
        }

        // Remove half of the keys.
        for (int i = 0; i < kvs.size() / 2; i++) {
            ASSERT_EQ(bc.remove(kvs[i].first).get(), true);
        }

        // Remove again, should fail.
        for (int i = 0; i < kvs.size() / 2; i++) {
            ASSERT_EQ(bc.remove(kvs[i].first).get(), false);
        }

        // Get should return no value.
        for (int i = 0; i < kvs.size() / 2; i++) {
            ASSERT_EQ(bc.get(kvs[i].first).has_value(), false);
        }

        // Validate non deleted kvs.
        for (int i = kvs.size() / 2; i < kvs.size(); i++) {
            ASSERT_EQ(bc.get(kvs[i].first).value(), kvs[i].second);
        }
    }

    {
        BitCask bc(test_dir_, Params{});
        for (int i = 0; i < kvs.size() / 2; i++) {
            ASSERT_EQ(bc.get(kvs[i].first).has_value(), false);
        }

        for (int i = kvs.size() / 2; i < kvs.size(); i++) {
            ASSERT_EQ(bc.get(kvs[i].first).value(), kvs[i].second);
        }
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}