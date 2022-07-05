/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>

#include <hbase/hbase.h>

/* Found under /libhbase/src/test/native/common */
#include "byte_buffer.h"

// #line __LINE__ "example_async.c"
#line __LINE__ "main.cpp"

/*
 * Sample code to illustrate usage of libhbase APIs
 */

#ifdef __cplusplus
extern  "C" {
#endif

#define CHECK_API_ERROR(retCode, ...) \
    HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO), \
        __VA_ARGS__, retCode);

static bytebuffer cf1 = bytebuffer_strcpy("test_cf1");
static bytebuffer cf2 = bytebuffer_strcpy("test_cf2");

static void printRow(const hb_result_t result) {
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

/**
 *  * Get synchronizer and callback
 *   */
static volatile bool get_done = false;
static pthread_cond_t get_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t get_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
get_callback(int32_t err, hb_client_t client,
    hb_get_t get, hb_result_t result, void *extra) {
  bytebuffer rowKey = (bytebuffer)extra;
  if (err == 0) {
    const char *table_name;
    size_t table_name_len;
    hb_result_get_table(result, &table_name, &table_name_len);
    HBASE_LOG_INFO("Received get callback for table=\'%.*s\'.",
        table_name_len, table_name);

    printRow(result);

    const hb_cell_t *mycell;
    bytebuffer qualifier = bytebuffer_strcpy("test_q1");
    HBASE_LOG_INFO("Looking up cell for family=\'%s\', qualifier=\'%.*s\'.",
        cf1->buffer, qualifier->length, qualifier->buffer);
    if (hb_result_get_cell(result, cf1->buffer, cf1->length, qualifier->buffer,
        qualifier->length, &mycell) == 0) {
      HBASE_LOG_INFO("Cell found, value=\'%.*s\', timestamp=%lld.",
          mycell->value_len, mycell->value, mycell->ts);
    } else {
      HBASE_LOG_ERROR("Cell not found.");
    }
    bytebuffer_free(qualifier);
    hb_result_destroy(result);
  } else {
    HBASE_LOG_ERROR("Get failed with error code: %d.", err);
  }

  bytebuffer_free(rowKey);
  hb_get_destroy(get);

  pthread_mutex_lock(&get_mutex);
  get_done = true;
  pthread_cond_signal(&get_cv);
  pthread_mutex_unlock(&get_mutex);
}

static void
wait_for_get() {
  HBASE_LOG_INFO("Waiting for get operation to complete.");
  pthread_mutex_lock(&get_mutex);
  while (!get_done) {
    pthread_cond_wait(&get_cv, &get_mutex);
  }
  pthread_mutex_unlock(&get_mutex);
  HBASE_LOG_INFO("Get operation completed.");
}

/**
 * Client destroy synchronizer and callbacks
 */
static volatile bool client_destroyed = false;
static pthread_cond_t client_destroyed_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t client_destroyed_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
client_disconnection_callback(int32_t err,
    hb_client_t client, void *extra) {
  HBASE_LOG_INFO("Received client disconnection callback.");
  pthread_mutex_lock(&client_destroyed_mutex);
  client_destroyed = true;
  pthread_cond_signal(&client_destroyed_cv);
  pthread_mutex_unlock(&client_destroyed_mutex);
}

static void
wait_client_disconnection() {
  HBASE_LOG_INFO("Waiting for client to disconnect.");
  pthread_mutex_lock(&client_destroyed_mutex);
  while (!client_destroyed) {
    pthread_cond_wait(&client_destroyed_cv, &client_destroyed_mutex);
  }
  pthread_mutex_unlock(&client_destroyed_mutex);
  HBASE_LOG_INFO("Client disconnected.");
}

/**
 * Program entry point
 */
int
main(int argc, char **argv) {
  int32_t retCode = 0;
  FILE* logFile = NULL;
  hb_connection_t connection = NULL;
  hb_client_t client = NULL;
  const char *rowkey_prefix = "rowkey";
  const char *value_prefix = "test value";
  bytebuffer cf1 = bytebuffer_strcpy("test_cf1");
  bytebuffer cf2 = bytebuffer_strcpy("test_cf2");
  bytebuffer column_a = bytebuffer_strcpy("test_q1");
  bytebuffer column_b = bytebuffer_strcpy("test_q2");

  const char *table_name      = (argc > 1) ? argv[1] : "test_table";
  const char *zk_ensemble     = (argc > 2) ? argv[2] : "adm1.hdp.io:2181,hdm1.hdp.io:2181,hdm2.hdp.io:2181";
  const char *zk_root_znode   = (argc > 3) ? argv[3] : "/hbase-unsecure";
  const size_t table_name_len = strlen(table_name);

  srand(time(NULL));
  hb_log_set_level(HBASE_LOG_LEVEL_DEBUG); // defaults to INFO
  const char *logFilePath = getenv("HBASE_LOG_FILE");
  if (logFilePath != NULL) {
    FILE* logFile = fopen(logFilePath, "a");
    if (!logFile) {
      retCode = errno;
      fprintf(stderr, "Unable to open log file \"%s\"", logFilePath);
      perror(NULL);
      goto cleanup;
    }
    hb_log_set_stream(logFile); // defaults to stderr
  }

  if ((retCode = hb_connection_create(zk_ensemble,
                                      zk_root_znode,
                                      &connection)) != 0) {
    HBASE_LOG_ERROR("Could not create HBase connection : errorCode = %d.", retCode);
    goto cleanup;
  }

//  if ((retCode = ensureTable(connection, table_name)) != 0) {
//    HBASE_LOG_ERROR("Failed to ensure table %s : errorCode = %d", table_name, retCode);
//    goto cleanup;
//  }

  HBASE_LOG_INFO("Connecting to HBase cluster using Zookeeper ensemble '%s'.",
                 zk_ensemble);
  if ((retCode = hb_client_create(connection, &client)) != 0) {
    HBASE_LOG_ERROR("Could not connect to HBase cluster : errorCode = %d.", retCode);
    goto cleanup;
  }

  // fetch a row with row-key="row_with_two_cells"
  {
    bytebuffer rowKey = bytebuffer_strcpy("rowkey0");
    hb_get_t get = NULL;
    hb_get_create(rowKey->buffer, rowKey->length, &get);
    hb_get_add_column(get, cf1->buffer, cf1->length, NULL, 0);
    hb_get_add_column(get, cf2->buffer, cf2->length, NULL, 0);
    hb_get_set_table(get, table_name, table_name_len);
    hb_get_set_num_versions(get, 10); // up to ten versions of each column

    get_done = false;
    hb_get_send(client, get, get_callback, rowKey);
    wait_for_get();
  }

  // fetch a row with row-key="row_with_two_cells"
  {
    bytebuffer rowKey = bytebuffer_strcpy("rowkey1");
    hb_get_t get = NULL;
    hb_get_create(rowKey->buffer, rowKey->length, &get);
    hb_get_add_column(get, cf1->buffer, cf1->length, NULL, 0);
    hb_get_add_column(get, cf2->buffer, cf2->length, NULL, 0);
    hb_get_set_table(get, table_name, table_name_len);
    hb_get_set_num_versions(get, 10); // up to ten versions of each column

    get_done = false;
    hb_get_send(client, get, get_callback, rowKey);
    wait_for_get();
  }

cleanup:
  if (client) {
    HBASE_LOG_INFO("Disconnecting client.");
    hb_client_destroy(client, client_disconnection_callback, NULL);
    wait_client_disconnection();
  }

  if (connection) {
    hb_connection_destroy(connection);
  }

  if (column_a) {
    bytebuffer_free(column_a);  // do not need 'column' anymore
  }
  if (column_b) {
    bytebuffer_free(column_b);
  }

  if (logFile) {
    fclose(logFile);
  }

//  pthread_cond_destroy(&puts_cv);
//  pthread_mutex_destroy(&puts_mutex);

  pthread_cond_destroy(&get_cv);
  pthread_mutex_destroy(&get_mutex);

//  pthread_cond_destroy(&del_cv);
//  pthread_mutex_destroy(&del_mutex);

  pthread_cond_destroy(&client_destroyed_cv);
  pthread_mutex_destroy(&client_destroyed_mutex);

  return retCode;
}

#ifdef __cplusplus
}
#endif
[ddadmin@build:~/Downloads/hbase-native-client-test/build]$
