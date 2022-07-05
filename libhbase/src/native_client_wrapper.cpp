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

NativeClientWrapper::NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name)
        : NativeClientWrapper(std::move(zk_quorum), std::move(zk_znode_parent), std::move(table_name), ',') {}

NativeClientWrapper::NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name, char delimiter) {
    this->zk_quorum = std::move(zk_quorum);
    this->zk_znode_parent = std::move(zk_znode_parent);
    this->table_name = std::move(table_name);
    this->delimiter = delimiter;
    this->setup();
}

void NativeClientWrapper::setup() {
    this->get_done = false;
    this->get_cv = PTHREAD_COND_INITIALIZER;
    this->get_mutex = PTHREAD_MUTEX_INITIALIZER;

    this->client_destroyed = false;
    this->client_destroyed_cv = PTHREAD_COND_INITIALIZER;
    this->client_destroyed_mutex = PTHREAD_MUTEX_INITIALIZER;

    this->connection = NULL;
    this->client = NULL;
    hb_log_set_level(HBASE_LOG_LEVEL_DEBUG); // defaults to INFO

    if ((this->ret_code = hb_connection_create(this->zk_quorum.c_str(), this->zk_znode_parent.c_str(), &this->connection)) != 0) {
        HBASE_LOG_ERROR("Could not create HBase connection : errorCode = %d.", ret_code);
        this->cleanup();
    }

    HBASE_LOG_INFO("Connecting to HBase cluster using Zookeeper ensemble '%s'.", this->zk_quorum);
    if ((this->ret_code = hb_client_create(this->connection, &this->client)) != 0) {
        HBASE_LOG_ERROR("Could not connect to HBase cluster : errorCode = %d.", this->ret_code);
        this->cleanup();
    }
}

int32_t NativeClientWrapper::cleanup() {
    if (this->client) {
        HBASE_LOG_INFO("Disconnecting client.");
        hb_client_destroy(this->client, this->client_disconnection_callback, NULL);
        this->wait_client_disconnection();
    }

    if (this->connection) {
        hb_connection_destroy(this->connection);
    }

    // pthread_cond_destroy(&puts_cv);
    // pthread_mutex_destroy(&puts_mutex);

    pthread_cond_destroy(&this->get_cv);
    pthread_mutex_destroy(&this->get_mutex);

    // pthread_cond_destroy(&del_cv);
    // pthread_mutex_destroy(&del_mutex);

    pthread_cond_destroy(&this->client_destroyed_cv);
    pthread_mutex_destroy(&this->client_destroyed_mutex);

    return this->ret_code;
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

                        // if (cf1) {
                        //     bytebuffer_free(cf1);
                        // }
                        // if (column_b) {
                        //     bytebuffer_free(column_b);
                        // }
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

int main(int argc, char **argv) {
    const char *zk_quorum_arg = (argc > 1) ? argv[1] : "adm1.hdp.io,hdm1.hdp.io,hdm2.hdp.io";
    const char *zk_znode_parent_arg = (argc > 2) ? argv[2] : "/hbase-unsecure";
    const char *table_name_arg = (argc > 3) ? argv[3] : "test_table";
    const char *rowkeys_arg = (argc > 4) ? argv[4] : "rowkey0,rowkey1";
    const char *cfs_arg = (argc > 5) ? argv[5] : "test_cf1,test_cf2";
    const char *qs_arg = (argc > 6) ? argv[6] : "test_q1,test_q2,test_q5,test_q22";
    std::string table_name(table_name_arg);
    std::string rowkeys(rowkeys_arg);
    std::string cfs(cfs_arg);
    std::string qs(qs_arg);

    NativeClientWrapper wrapper(zk_quorum_arg, zk_znode_parent_arg, table_name_arg);

    wrapper.gets(rowkeys_arg);
    wrapper.gets(rowkeys_arg, cfs_arg);
    wrapper.gets(rowkeys_arg, cfs_arg, qs_arg);

    wrapper.gets(rowkeys);
    wrapper.gets(rowkeys, cfs);
    wrapper.gets(rowkeys, cfs, qs);
}
