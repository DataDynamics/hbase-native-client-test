#include "native_client_wrapper.hpp"
#include <iostream>
#include <string>

using namespace std;

void print_value(int x) {
    cout << x << endl;
}

int main(int argc, char **argv) {
  const char arg1 = (argc > 1) ? argv[1] : "111";
//  const char *zk_ensemble = (argc > 2) ? argv[2] : "adm1.hdp.io:2181,hdm1.hdp.io:2181,hdm2.hdp.io:2181";
//  const char *zk_root_znode = (argc > 3) ? argv[3] : "/hbase-unsecure";
  std::string str(arg1);
  int i = std::stoi(str);
  print_value(i);
}
