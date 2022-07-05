#include "native_client_wrapper.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>

using namespace std;

void print_value(int x) {
    cout << x << endl;
}

vector<string> split(string input, char delimiter) {
    vector<string> answer;
    stringstream ss(input);
    string temp;
    while (getline(ss, temp, delimiter)) {
        answer.push_back(temp);
    }
    return answer;
}

void gets(string *rowkey, string *family) {

}

int main(int argc, char **argv) {
    const char *rowkeys_arg = (argc > 1) ? argv[1] : "rowkey0,rowkey1";
    const char *cfs_arg = (argc > 2) ? argv[2] : "test_cf1,test_cf2";
    const char *qs_arg = (argc > 3) ? argv[3] : "test_q1,test_q2,test_q5,test_q22";
//    const char *zk_ensemble = (argc > 2) ? argv[2] : "adm1.hdp.io:2181,hdm1.hdp.io:2181,hdm2.hdp.io:2181";
//    const char *zk_root_znode = (argc > 3) ? argv[3] : "/hbase-unsecure";
    std::string rowkeys(rowkeys_arg);
    std::string cfs(cfs_arg);
    std::string qs(qs_arg);
    cout << "rowkeys = " << rowkeys << endl;
    const vector<string> rowkey_vector = split(rowkeys, ',');
    for (int i = 0; i < rowkey_vector.size(); i++) {
        cout << "rowkey_vector[" << i << "] = " << rowkey_vector[i] << " ";
    }
    const vector<string> cf_vector = split(cfs, ',');
    for (int i = 0; i < cf_vector.size(); i++) {
        cout << "cf_vector[" << i << "] = " << cf_vector[i] << " ";
    }
    const vector<string> q_vector = split(qs, ',');
    for (int i = 0; i < q_vector.size(); i++) {
        cout << "q_vector[" << i << "] = " << q_vector[i] << " ";
    }
}
