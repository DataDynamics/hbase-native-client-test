#include "native_client_wrapper.hpp"
#include <iostream>
#include <cstring>
#include <utility>
#include <vector>

#include <hbase/hbase.h>

#include "byte_buffer.h"

#line __LINE__ "src/native_client_wrapper.cpp"

#define CHECK_API_ERROR(retCode, ...) \
    HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO), __VA_ARGS__, retCode);

using namespace std;

NativeClientWrapper::NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name)
        : NativeClientWrapper(std::move(zk_quorum), std::move(zk_znode_parent), std::move(table_name), ',') {}

NativeClientWrapper::NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name, char delimiter) {
    this->zk_quorum = std::move(zk_quorum);
    this->zk_znode_parent = std::move(zk_znode_parent);
    this->table_name = std::move(table_name);
    this->table_name_len = strlen(this->table_name.c_str());
    this->delimiter = delimiter;
    hb_log_set_level(HBASE_LOG_LEVEL_DEBUG); // defaults to INFO

    if ((this->ret_code = hb_connection_create(this->zk_quorum.c_str(), this->zk_znode_parent.c_str(),
                                               &this->connection)) != 0) {
        HBASE_LOG_ERROR("Could not create HBase connection : errorCode = %d.", this->ret_code);
        this->cleanup();
    }

    HBASE_LOG_INFO("Connecting to HBase cluster using Zookeeper ensemble '%s'.", this->zk_quorum.c_str());
    if ((this->ret_code = hb_client_create(this->connection, &this->client)) != 0) {
        HBASE_LOG_ERROR("Could not connect to HBase cluster : errorCode = %d.", this->ret_code);
        this->cleanup();
    }
}

void NativeClientWrapper::gets(const vector<string> &rowkeys, const vector<string> &families,
                               const vector<string> &qualifiers) {
    vector<hb_get_t> gets;
    for (const string &rowkey: rowkeys) {
        bytebuffer r_buffer = bytebuffer_strcpy(rowkey.c_str());
        hb_get_t get = NULL;
        hb_get_create(r_buffer->buffer, r_buffer->length, &get);
        hb_get_set_table(get, this->table_name.c_str(), this->table_name_len);
        // hb_get_set_num_versions(get, 10); // up to ten versions of each column
        if (families.empty()) {
            HBASE_LOG_DEBUG("row=%s", rowkey.c_str());
            // cout << "rowkey" << "=" << rowkey << endl;
        } else {
            for (const string &family: families) {
                bytebuffer f_buffer = bytebuffer_strcpy(family.c_str());
                if (qualifiers.empty()) {
                    HBASE_LOG_DEBUG("rowkey:family=%s:%s", rowkey.c_str(), family.c_str());
                    // cout << "rowkey:family" << "=" << rowkey << ":" << family << endl;
                    hb_get_add_column(get, f_buffer->buffer, f_buffer->length, NULL, 0);
                } else {
                    for (const string &qualifier: qualifiers) {
                        HBASE_LOG_DEBUG("rowkey:family:qualifier=%s:%s:%s", rowkey.c_str(), family.c_str(),
                                        qualifier.c_str());
                        // cout << "rowkey:family:qualifier" << "=" << rowkey << ":" << family << ":" << qualifier << endl;
                        bytebuffer q_buffer = bytebuffer_strcpy(qualifier.c_str());
                        hb_get_add_column(get, f_buffer->buffer, f_buffer->length, q_buffer->buffer, q_buffer->length);
                        if (q_buffer) {
                            bytebuffer_free(q_buffer);
                        }
                    }
                }
                if (f_buffer) {
                    bytebuffer_free(f_buffer);
                }
            }
        }
        gets.push_back(get);

        NativeClientWrapper::get_done = false;
        hb_get_send(this->client, get, get_callback, r_buffer);
        wait_for_get();

        if (r_buffer) {
            bytebuffer_free(r_buffer);
        }
    }
}

void NativeClientWrapper::get_callback(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra) {
    // bytebuffer r_buffer = (bytebuffer) extra;
    if (err == 0) {
        // const char *table_name;
        // size_t table_name_len;
        // hb_result_get_table(result, &table_name, &table_name_len);
        // HBASE_LOG_INFO("Received get callback for table=\'%.*s\'.", table_name_len, table_name);

        process_row(result);

        const hb_cell_t *mycell;
        // bytebuffer qualifier = bytebuffer_strcpy("test_q1");
        // HBASE_LOG_INFO("Looking up cell for family=\'%s\', qualifier=\'%.*s\'.", cf1->buffer, qualifier->length, qualifier->buffer);
        // if (hb_result_get_cell(result, cf1->buffer, cf1->length, qualifier->buffer, qualifier->length, &mycell) == 0) {
        //     HBASE_LOG_INFO("Cell found, value=\'%.*s\', timestamp=%lld.", mycell->value_len, mycell->value, mycell->ts);
        // } else {
        //     HBASE_LOG_ERROR("Cell not found.");
        // }
        // bytebuffer_free(qualifier);
        hb_result_destroy(result);
    } else {
        HBASE_LOG_ERROR("Get failed with error code: %d.", err);
    }

    // bytebuffer_free(r_buffer);
    hb_get_destroy(get);

    pthread_mutex_lock(&NativeClientWrapper::get_mutex);
    NativeClientWrapper::get_done = true;
    pthread_cond_signal(&NativeClientWrapper::get_cv);
    pthread_mutex_unlock(&NativeClientWrapper::get_mutex);
}

void NativeClientWrapper::process_row(hb_result_t result) {
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

int main(int argc, char **argv) {
    const char *zk_quorum_arg = "adm1.hdp.io,hdm1.hdp.io,hdm2.hdp.io";
    const char *zk_znode_parent_arg = "/hbase-unsecure";
    int index = 0;
    const char *program_name_arg = argv[index];
    index++;
    cout << "program_name_arg = " << program_name_arg << endl;
    const char *table_name_arg = (argc > index) ? argv[index] : "test_table";
    index++;
    const char *rowkeys_arg = (argc > index) ? argv[index] : "rowkey0,rowkey1";
    index++;
    const char *cfs_arg = (argc > index) ? argv[index] : "test_cf1,test_cf2";
    index++;
    const char *qs_arg = (argc > index) ? argv[index] : "test_q1,test_q2,test_q5,test_q22";
    index++;
    std::string table_name(table_name_arg);
    std::string rowkeys(rowkeys_arg);
    std::string cfs(cfs_arg);
    std::string qs(qs_arg);

    NativeClientWrapper wrapper(zk_quorum_arg, zk_znode_parent_arg, table_name_arg);

    // wrapper.gets(rowkeys_arg);
    // wrapper.gets(rowkeys_arg, cfs_arg);
    // wrapper.gets(rowkeys_arg, cfs_arg, qs_arg);

    wrapper.gets(rowkeys);
    wrapper.gets(rowkeys, cfs);
    wrapper.gets(rowkeys, cfs, qs);
}
