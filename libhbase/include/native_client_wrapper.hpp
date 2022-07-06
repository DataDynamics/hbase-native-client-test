#include <iostream>
#include <vector>
#include <sstream>
#include <hbase/hbase.h>

#line __LINE__ "src/native_client_wrapper.hpp"

using namespace std;

#define CHECK_API_ERROR(retCode, ...) \
    HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO), __VA_ARGS__, retCode);

/**
 * Get synchronizer and callback
 */
static volatile bool get_done = false;
static pthread_cond_t get_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t get_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * Client destroy synchronizer and callbacks
 */
static volatile bool client_destroyed = false;
static pthread_cond_t client_destroyed_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t client_destroyed_mutex = PTHREAD_MUTEX_INITIALIZER;

class NativeClientWrapper {
private:
    int32_t ret_code = 0;
    string zk_quorum;
    string zk_znode_parent;
    static string table_name;
    static size_t table_name_len;
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

    ~NativeClientWrapper() {
        this->cleanup();
    }

    static void get_callback(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra) {
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

        pthread_mutex_lock(&get_mutex);
        get_done = true;
        pthread_cond_signal(&get_cv);
        pthread_mutex_unlock(&get_mutex);
    }

    static void process_row(hb_result_t result);

    int32_t cleanup() {
        if (this->client) {
            HBASE_LOG_INFO("Disconnecting client.");
            // hb_client_destroy(this->client, this->client_disconnection_callback, NULL);
            // wait_client_disconnection();
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
        gets(rowkeys, families, this->dummy);
    }

    /**
     * rowkey, column family, column qualifier 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     * @param qualifiers qualifier 목록
     */
    void gets(const string &rowkeys, const string &families, const string &qualifiers) {
        gets(this->split(rowkeys), this->split(families), this->split(qualifiers));
    }

    /**
     * rowkey, column family, column qualifier 를 지정하여 get 한다.
     *
     * @param rowkeys rowkey 목록
     * @param families column family 목록
     * @param qualifiers qualifier 목록
     */
    static void gets(const vector<string> &rowkeys, const vector<string> &families, const vector<string> &qualifiers);
};

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
