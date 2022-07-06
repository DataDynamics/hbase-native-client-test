#include <iostream>
#include <vector>
#include <sstream>
#include <hbase/hbase.h>

#line __LINE__ "src/native_client_wrapper.hpp"

using namespace std;

#define CHECK_API_ERROR(retCode, ...) \
    HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO), __VA_ARGS__, retCode);

class NativeClientWrapper {
private:
    int32_t ret_code = 0;
    string zk_quorum;
    string zk_znode_parent;
    string table_name;
    size_t table_name_len;
    char delimiter;
    vector<string> dummy;

    hb_connection_t connection;
    hb_client_t client;

    vector<string> split(const string &input) const {
        vector<string> answer;
        stringstream ss(input);
        string temp;
        while (getline(ss, temp, this->delimiter)) {
            answer.push_back(temp);
        }
        return answer;
    }

public:
    /**
     * Get synchronizer and callback
     */
    static volatile bool get_done;
    static pthread_cond_t get_cv;
    static pthread_mutex_t get_mutex;

    /**
     * Client destroy synchronizer and callbacks
     */
    static volatile bool client_destroyed;
    static pthread_cond_t client_destroyed_cv;
    static pthread_mutex_t client_destroyed_mutex;

    explicit NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name);

    NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name, char delimiter);

    ~NativeClientWrapper();

    static void wait_for_get() {
        HBASE_LOG_INFO("Waiting for get operation to complete.");
        pthread_mutex_lock(&get_mutex);
        while (!get_done) {
            pthread_cond_wait(&get_cv, &get_mutex);
        }
        pthread_mutex_unlock(&get_mutex);
        HBASE_LOG_INFO("Get operation completed.");
    }

    static void wait_client_disconnection() {
        HBASE_LOG_INFO("Waiting for client to disconnect.");
        pthread_mutex_lock(&client_destroyed_mutex);
        while (!client_destroyed) {
            pthread_cond_wait(&client_destroyed_cv, &client_destroyed_mutex);
        }
        pthread_mutex_unlock(&client_destroyed_mutex);
        HBASE_LOG_INFO("Client disconnected.");
    }

    static void client_disconnection_callback(int32_t err, hb_client_t client, void *extra) {
        HBASE_LOG_INFO("Received client disconnection callback.");
        pthread_mutex_lock(&client_destroyed_mutex);
        client_destroyed = true;
        pthread_cond_signal(&client_destroyed_cv);
        pthread_mutex_unlock(&client_destroyed_mutex);
    }

    void setup();

    int32_t cleanup();

    void get_callback(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra);

    /**
     * rowkey 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     */
    void gets(const string &rowkeys);

    /**
     * rowkey 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     */
    void gets(const vector<string> &rowkeys);

    /**
     * rowkey, column family 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     */
    void gets(const string &rowkeys, const string &families);

    /**
     * rowkey, column family 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     */
    void gets(const vector<string> &rowkeys, const vector<string> &families);

    /**
     * rowkey, column family, column qualifier 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     * @param qualifiers qualifier 목록
     */
    void gets(const string &rowkeys, const string &families, const string &qualifiers);

    /**
     * rowkey, column family, column qualifier 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     * @param qualifiers qualifier 목록
     */
    void gets(const vector<string> &rowkeys, const vector<string> &families, const vector<string> &qualifiers);

    /**
     * rowkey, column family, column qualifier 를 지정하여 get 한다.
     *
     * @param gets get object 목록
     */
    void gets(const vector<hb_get_t> &gets);

    void process_row(hb_result_t result);
};
