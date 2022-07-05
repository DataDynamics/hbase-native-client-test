#include <iostream>
#include <vector>

using namespace std;

#ifndef LIB_HBASE_CLIENT_INCLUDED
#define LIB_HBASE_CLIENT_INCLUDED

#ifdef __cplusplus
    extern "C" {
#endif

void print_value(int x);

void hbase_gets(vector<string>* rowkeys, vector<string>* families);

void hbase_gets(vector<string>* rowkeys, vector<string>* families, vector<string>* qualifiers);

#ifdef __cplusplus
    }
#endif
#endif /* LIB_HBASE_CLIENT_INCLUDED */
