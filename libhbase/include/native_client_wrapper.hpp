#include <iostream>
#include <vector>

using namespace std;

void print_value(int x);

/**
 * rowkey 를 지정하여 get 한다.
 *
 * @param rowkeys rowkey 목록
 */
void gets(vector<string> *rowkeys);

/**
 * rowkey, column family 를 지정하여 get 한다.
 *
 * @param rowkeys rowkey 목록
 * @param families column family 목록
 */
void gets(vector<string> *rowkeys, vector<string> *families);

/**
 * rowkey, column family, column qualifier 를 지정하여 get 한다.
 *
 * @param rowkeys rowkey 목록
 * @param families column family 목록
 * @param qualifiers qualifier 목록
 */
void gets(vector<string> *rowkeys, vector<string> *families, vector<string> *qualifiers);
