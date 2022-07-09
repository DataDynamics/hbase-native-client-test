#include <iostream>
#include <utility>
#include <cstring>
#include <vector>
#include <sstream>
#include <hbase/hbase.h>
#include "byte_buffer.h"

#line __LINE__ "src/native_client_wrapper.hpp"

using namespace std;

// #define CHECK_API_ERROR(retCode, ...) HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO), __VA_ARGS__, retCode);

class NativeClientWrapper {
private:
    int32_t ret_code = 0;
    string zk_quorum;
    string zk_znode_parent;
    string table_name;
    size_t table_name_len;
    char delimiter;
    vector<string> dummy;

    hb_connection_t connection{};
    hb_client_t client{};

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

    NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name_);
    // NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name_)
    //         : NativeClientWrapper(
    //         std::move(zk_quorum),
    //         std::move(zk_znode_parent),
    //         std::move(table_name_),
    //         ',') {}

    NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name_, char delimiter);
    // NativeClientWrapper(string zk_quorum, string zk_znode_parent, string table_name_, char delimiter) {
    //     this->zk_quorum = std::move(zk_quorum);
    //     this->zk_znode_parent = std::move(zk_znode_parent);
    //     this->table_name = std::move(table_name_);
    //     this->table_name_len = strlen(table_name.c_str());
    //     this->delimiter = delimiter;
    //     // hb_log_set_level(HBASE_LOG_LEVEL_DEBUG);// defaults to DEBUG
    //     hb_log_set_level(HBASE_LOG_LEVEL_INFO);
    //
    //     if ((this->ret_code = hb_connection_create(this->zk_quorum.c_str(), this->zk_znode_parent.c_str(),
    //                                                &this->connection)) != 0) {
    //         HBASE_LOG_ERROR("Could not create HBase connection : errorCode = %d.", this->ret_code);
    //         this->cleanup();
    //     }
    //
    //     HBASE_LOG_INFO("Connecting to HBase cluster using Zookeeper ensemble '%s'.", this->zk_quorum.c_str());
    //     if ((this->ret_code = hb_client_create(this->connection, &this->client)) != 0) {
    //         HBASE_LOG_ERROR("Could not connect to HBase cluster : errorCode = %d.", this->ret_code);
    //         this->cleanup();
    //     }
    // }

    ~NativeClientWrapper() {
        this->cleanup();
    }

    static void get_callback(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra);

    static void process_row(hb_result_t result);

    static void wait_for_get() {
        HBASE_LOG_INFO("Waiting for get operation to complete.");
        pthread_mutex_lock(&get_mutex);
        HBASE_LOG_DEBUG("Waiting for get operation to complete. mutex locked");
        while (!get_done) {
            HBASE_LOG_DEBUG("Waiting for get operation to complete. get_done=%d", get_done);
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

    int32_t cleanup() {
        if (this->client) {
            HBASE_LOG_INFO("Disconnecting client.");
            // hb_client_destroy(this->client, client_disconnection_callback, NULL);
            hb_client_destroy(this->client, client_disconnection_callback, nullptr);
            wait_client_disconnection();
        }

        if (this->connection) {
            hb_connection_destroy(this->connection);
        }

        // pthread_cond_destroy(&puts_cv);
        // pthread_mutex_destroy(&puts_mutex);

        pthread_cond_destroy(&get_cv);
        pthread_mutex_destroy(&get_mutex);

        // pthread_cond_destroy(&del_cv);
        // pthread_mutex_destroy(&del_mutex);

        pthread_cond_destroy(&client_destroyed_cv);
        pthread_mutex_destroy(&client_destroyed_mutex);

        return this->ret_code;
    }

    /**
     * rowkey 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     */
    void gets(const string &rowkeys) {
        this->gets(this->split(rowkeys));
    }

    /**
     * rowkey 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     */
    void gets(const vector<string> &rowkeys) {
        this->gets(rowkeys, this->dummy);
    }

    /**
     * rowkey, column family 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     */
    void gets(const string &rowkeys, const string &families) {
        this->gets(this->split(rowkeys), this->split(families));
    }

    /**
     * rowkey, column family 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     */
    void gets(const vector<string> &rowkeys, const vector<string> &families) {
        this->gets(rowkeys, families, this->dummy);
    }

    /**
     * rowkey, column family, column qualifier 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     * @param qualifiers qualifier 목록
     */
    void gets(const string &rowkeys, const string &families, const string &qualifiers) {
        this->gets(this->split(rowkeys), this->split(families), this->split(qualifiers));
    }

    /**
     * rowkey, column family, column qualifier 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     * @param qualifiers qualifier 목록
     */
    void gets(const vector<string> &rowkeys, const vector<string> &families, const vector<string> &qualifiers);
};

/**
 * Get synchronizer and callback
 */
volatile bool NativeClientWrapper::get_done = false;
pthread_cond_t NativeClientWrapper::get_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t NativeClientWrapper::get_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * Client destroy synchronizer and callbacks
 */
volatile bool NativeClientWrapper::client_destroyed = false;
pthread_cond_t NativeClientWrapper::client_destroyed_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t NativeClientWrapper::client_destroyed_mutex = PTHREAD_MUTEX_INITIALIZER;
