#include "native_client_wrapper.hpp"
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <hbase/hbase.h>

#include "byte_buffer.h"

using namespace std;

void print_value(int x) {
    cout << x << endl;
}

NativeClientWrapper::NativeClientWrapper(string table_name) : NativeClientWrapper(std::move(table_name), ',') {}

NativeClientWrapper::NativeClientWrapper(string table_name, char delimiter) {
    this->table_name = std::move(table_name);
    this->delimiter = delimiter;
}

void NativeClientWrapper::print_value(int x) {

}

void NativeClientWrapper::gets(const string &rowkeys) {
    this->gets(this->split(rowkeys));
}

void NativeClientWrapper::gets(const vector<string> &rowkeys) {
    this->gets(rowkeys, this->dummy);
}

void NativeClientWrapper::gets(const string &rowkeys, const string &families) {
    this->gets(this->split(rowkeys), this->split(families));
}

void NativeClientWrapper::gets(const vector<string> &rowkeys, const vector<string> &families) {
    this->gets(rowkeys, families, this->dummy);
}

void NativeClientWrapper::gets(const string &rowkeys, const string &families, const string &qualifiers) {
    this->gets(this->split(rowkeys), this->split(families), this->split(qualifiers));
}

void NativeClientWrapper::gets(const vector<string> &rowkeys, const vector<string> &families,
                               const vector<string> &qualifiers) {
    for (const string &rowkey: rowkeys) {
        if (families.empty()) {
            cout << "rowkey" << "=" << rowkey << endl;
        } else {
            for (const string &family: families) {
                if (qualifiers.empty()) {
                    cout << "rowkey:family" << "=" << rowkey << ":" << family << endl;
                } else {
                    for (const string &qualifier: qualifiers) {
                        cout << "rowkey:family:qualifier" << "=" << rowkey << ":" << family << ":" << qualifier << endl;
                        // bytebuffer cf1 = bytebuffer_strcpy(family.c_str());
                        // bytebuffer column_b = bytebuffer_strcpy(qualifier.c_str());
                    }
                }
            }
        }
    }
}

// void get() {
//     hb_connection_t connection = NULL;
//     hb_client_t client = NULL;
// }

int main(int argc, char **argv) {
    const char *table_name_arg = (argc > 1) ? argv[1] : "test_table";
    const char *rowkeys_arg = (argc > 2) ? argv[2] : "rowkey0,rowkey1";
    const char *cfs_arg = (argc > 3) ? argv[3] : "test_cf1,test_cf2";
    const char *qs_arg = (argc > 4) ? argv[4] : "test_q1,test_q2,test_q5,test_q22";
    std::string table_name(table_name_arg);
    std::string rowkeys(rowkeys_arg);
    std::string cfs(cfs_arg);
    std::string qs(qs_arg);

    NativeClientWrapper wrapper(table_name_arg);

    wrapper.gets(rowkeys_arg);
    wrapper.gets(rowkeys_arg, cfs_arg);
    wrapper.gets(rowkeys_arg, cfs_arg, qs_arg);

    wrapper.gets(rowkeys);
    wrapper.gets(rowkeys, cfs);
    wrapper.gets(rowkeys, cfs, qs);
}
