#include <iostream>

using namespace std;

#ifndef LIB_HBASE_CLIENT_INCLUDED
#define LIB_HBASE_CLIENT_INCLUDED

#ifdef __cplusplus
    extern "C" {
#endif

void print_value(int x);

void gets(string *rowkey, string *family);

#ifdef __cplusplus
    }
#endif
#endif /* LIB_HBASE_CLIENT_INCLUDED */
