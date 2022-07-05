#include "native_client_wrapper.hpp"
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <hbase/hbase.h>

#include "byte_buffer.h"

#define CHECK_API_ERROR(retCode, ...) \
    HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO), __VA_ARGS__, retCode);

using namespace std;

NativeClientWrapper::NativeClientWrapper(string table_name) : NativeClientWrapper(std::move(table_name), ',') {}

NativeClientWrapper::NativeClientWrapper(string table_name, char delimiter) {
    this->table_name = std::move(table_name);
    this->delimiter = delimiter;
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

void NativeClientWrapper::print_row(const hb_result_t result) {
    const byte_t *key = NULL;
    size_t key_len = 0;
    hb_result_get_key(result, &key, &key_len);
    size_t cell_count = 0;
    hb_result_get_cell_count(result, &cell_count);
    HBASE_LOG_INFO("Row=\'%.*s\', cell count=%d", key_len, key, cell_count);
    const hb_cell_t **cells;
    hb_result_get_cells(result, &cells, &cell_count);
    for (size_t i = 0; i < cell_count; ++i) {
        HBASE_LOG_INFO(
                "Cell %d: family=\'%.*s\', qualifier=\'%.*s\', "
                "value=\'%.*s\', timestamp=%lld.", i,
                cells[i]->family_len, cells[i]->family,
                cells[i]->qualifier_len, cells[i]->qualifier,
                cells[i]->value_len, cells[i]->value, cells[i]->ts);
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
