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

void gets(vector<string> rowkeys) {
    for (string rowkey: rowkeys) {
        cout << "rowkey" << "=" << rowkey << endl;
    }
}

void gets(vector<string> rowkeys, vector<string> families) {
    for (string rowkey: rowkeys) {
        for (auto family: families) {
            cout << "rowkey:family" << "=" << rowkey << ":" << family << endl;
        }
    }
}

void gets(vector<string> rowkeys, vector<string> families, vector<string> qualifiers) {
    for (string rowkey: rowkeys) {
        for (auto family: families) {
            if (qualifiers.empty()) {
                cout << "rowkey:family" << "=" << rowkey << ":" << family << endl;
            } else {
                for (auto qualifier: qualifiers) {
                    cout << "rowkey:family:qualifier" << "=" << rowkey << ":" << family << ":" << qualifier << endl;
                }
            }
        }
    }
}

int main(int argc, char **argv) {
    const char *rowkeys_arg = (argc > 1) ? argv[1] : "rowkey0,rowkey1";
    const char *cfs_arg = (argc > 2) ? argv[2] : "test_cf1,test_cf2";
    const char *qs_arg = (argc > 3) ? argv[3] : "test_q1,test_q2,test_q5,test_q22";
    std::string rowkeys(rowkeys_arg);
    std::string cfs(cfs_arg);
    std::string qs(qs_arg);
    const vector<string> rowkey_vector = split(rowkeys, ',');
    const vector<string> cf_vector = split(cfs, ',');
    const vector<string> q_vector = split(qs, ',');
    gets(rowkey_vector, cf_vector);
    gets(rowkey_vector, cf_vector, q_vector);
}
