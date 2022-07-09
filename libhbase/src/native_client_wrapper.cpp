#include "native_client_wrapper.hpp"
#include <iostream>
#include <cstring>
#include <hbase/hbase.h>

#line __LINE__ "src/native_client_wrapper.cpp"

using namespace std;

void NativeClientWrapper::process_row(hb_result_t result) {
    const byte_t *key = nullptr;
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
    // index++;
    std::string table_name(table_name_arg);
    std::string rowkeys(rowkeys_arg);
    std::string cfs(cfs_arg);
    std::string qs(qs_arg);

    NativeClientWrapper wrapper(zk_quorum_arg, zk_znode_parent_arg, table_name_arg);

    // wrapper.gets(rowkeys_arg);
    // wrapper.gets(rowkeys_arg, cfs_arg);
    // wrapper.gets(rowkeys_arg, cfs_arg, qs_arg);

    HBASE_LOG_INFO("try 1");
    wrapper.gets(rowkeys);
    HBASE_LOG_INFO("try 2");
    wrapper.gets(rowkeys, cfs);
    HBASE_LOG_INFO("try 3");
    wrapper.gets(rowkeys, cfs, qs);
}
