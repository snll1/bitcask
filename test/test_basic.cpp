
#include <bitcask.hpp>
#include <cassert>
#include <iostream>
#include <string>
using namespace std;
using namespace bitcask;

void basic_test() {
    {
        BitCask bc("./test_dir", Params{});
        for (int i = 0; i < 1e2; i++) {
            bc.put("sample" + to_string(i), "hello world " + to_string(i));
        }

        for (int i = 0; i < 1e2; i++) {
            cout << bc.get("sample" + to_string(i)).value() << endl;
        }
    }

    {
        BitCask bc("./test_dir", Params{});
        for (int i = 0; i < 1e2; i++) {
            cout << bc.get("sample" + to_string(i)).value() << endl;
        }
    }
}

int main(int argc, char **argv) {
    basic_test();
    return 0;
}