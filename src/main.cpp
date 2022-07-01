#include <iostream>
#include <string>
#include <map>
#include <vector>

#include "hbase/client/append.h"
#include "hbase/client/cell.h"
#include "hbase/client/client.h"
#include "hbase/client/configuration.h"
#include "hbase/client/delete.h"
#include "hbase/client/get.h"
#include "hbase/client/hbase-configuration-loader.h"
#include "hbase/client/increment.h"
#include "hbase/client/put.h"
#include "hbase/client/result.h"
#include "hbase/client/table.h"
#include "hbase/exceptions/exception.h"
#include "hbase/serde/table-name.h"
#include "hbase/test-util/mini-cluster-util.h"
#include "hbase/test-util/test-util.h"
#include "hbase/utils/bytes-util.h"
#include "hbase/utils/optional.h"
#include "server/zookeeper/ZooKeeper.pb.h"
#include "hbase/serde/region-info.h"
#include "hbase/serde/server-name.h"
#include "hbase/serde/zk.h"

using namespace std;
using hbase::BytesUtil;

// static const constexpr char *kDefHBaseConfPath = "./conf/";
// static const constexpr char *kHBaseDefaultXml = "hbase-default.xml";
// static const constexpr char *kHBaseSiteXml = "hbase-site.xml";
// static const constexpr char *kHBaseXmlData =
//         "<?xml version=\"1.0\"?>\n"
//         "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
//         "<!--\n"
//         "/**\n"
//         " *\n"
//         " * Licensed to the Apache Software Foundation (ASF) under one\n"
//         " * or more contributor license agreements.  See the NOTICE file\n"
//         " * distributed with this work for additional information\n"
//         " * regarding copyright ownership.  The ASF licenses this file\n"
//         " * to you under the Apache License, Version 2.0 (the\n"
//         " * \"License\"); you may not use this file except in compliance\n * "
//         "with the License.  You may obtain a copy of the License at\n"
//         " *\n"
//         " *     http://www.apache.org/licenses/LICENSE-2.0\n"
//         " *\n"
//         " * Unless required by applicable law or agreed to in writing, software\n"
//         " * distributed under the License is distributed on an \"AS IS\" BASIS,\n"
//         " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
//         " * See the License for the specific language governing permissions and\n"
//         " * limitations under the License.\n "
//         "*/\n"
//         "-->\n"
//         "<configuration>\n"
//         "    <property>\n"
//         "        <name>hbase.zookeeper.quorum</name>\n"
//         "        <value>adm1.dd.io,hdm1.dd.io,hdm2.dd.io</value>\n"
//         "        <description>IP 로 지정했더니 안됨</description>\n"
//         "    </property>\n"
//         "    <property>\n"
//         "        <name>zookeeper.znode.parent</name>\n"
//         "        <value>/hbase</value>\n"
//         "    </property>\n"
//         "</configuration>";


// static void WriteDataToFile(const std::string &file, const std::string &xml_data) {
//     std::ofstream hbase_conf;
//     hbase_conf.open(file.c_str());
//     hbase_conf << xml_data;
//     hbase_conf.close();
// }

// static void CreateHBaseConf(const std::string &dir, const std::string &file,
//                             const std::string xml_data) {
//     boost::filesystem::remove((dir + file).c_str());
//     boost::filesystem::create_directories(dir.c_str());
//     WriteDataToFile((dir + file), xml_data);
// }

// static void CreateHBaseConfWithEnv() {
//     CreateHBaseConf(kDefHBaseConfPath, kHBaseDefaultXml, kHBaseXmlData);
//
//     setenv("HBASE_CONF", kDefHBaseConfPath, 1);
// }

static void MakeGets(uint64_t num_rows, const std::string &row_prefix, std::vector <hbase::Get> &gets) {
    for (uint64_t i = 0; i < num_rows; ++i) {
        string row = row_prefix + std::to_string(i);
        hbase::Get get(row);
        get.AddFamily("test_cf1");// 특정 column family 의 모든 qualifier 지정
        get.AddColumn("test_cf2", "test_q22");// 특정 column family 의 특정 qualifier 지정
        // AddFamily, AddColumn 호출하지 않으면 모든 column family 의 모든 qualifier 를 가져옴
        gets.push_back(get);
    }
}

static void PrintMultiResults(uint64_t num_rows, const std::vector <std::shared_ptr<hbase::Result>> &results,
                              const std::vector <hbase::Get> &gets) {
    for (uint32_t i = 0; i < num_rows; ++i) {
        shared_ptr <hbase::Result> result = results[i];
        string rowkey = result->Row();

        // 모든 cell 출력
        cout << rowkey << ":*:*" << endl;
        vector <shared_ptr<hbase::Cell>> cells = result->Cells();
        vector < shared_ptr < hbase::Cell >> ::iterator
        cellIt = cells.begin();
        while (cellIt != cells.end()) {
            hbase::Cell *cell = cellIt->get();
            string rowkey = cell->Row();
            string family = cell->Family();
            string qualifier = cell->Qualifier();
            int64_t ts = cell->Timestamp();
            string value = cell->Value();
            cout << rowkey << ":" << family << ":" << qualifier << ":" << ts << "=" << value << endl;
            cellIt++;
        }
        cout << endl;

        // 특정 column family 의 모든 qualifier 출력
        cout << rowkey << ":test_cf1:*" << endl;
        map <string, string> familyMap = result->FamilyMap("test_cf1");// map<string, string>
        map<string, string>::iterator it = familyMap.begin();
        while (it != familyMap.end()) {
            string qualifier = it->first;
            string value = it->second;
            cout << rowkey << ":test_cf1:" << qualifier << "=" << value << endl;
            it++;
        }
        cout << endl;

        // 특정 family, qualifier 지정
        cout << rowkey << ":test_cf1:test_q1" << endl;
        hbase::optional <string> valueOp = result->Value("test_cf1", "test_q1");
        if (valueOp.is_initialized()) {
            cout << rowkey << ":test_cf1:test_q1=" << valueOp.value() << endl;
        }
        cout << endl;
    }
}

int main() {
    // CreateHBaseConfWithEnv();

    // vector <string> paths;
    // paths.push_back("hbase-site.xml");

    hbase::HBaseConfigurationLoader loader;
    // hbase::optional <hbase::Configuration> conf = loader.LoadResources("./conf/", paths);
    // /etc/hbase/conf/hbase-site.xml 또는 /etc/hbase/conf/hbase-default.xml 을 읽음
    hbase::optional <hbase::Configuration> conf = loader.LoadDefaultResources();

    std::string zk_node = hbase::ZKUtil::MetaZNode(*conf);
    cout << "zk_node = " << zk_node << endl;

    hbase::Client client(conf.value());

    hbase::pb::TableName tn = folly::to<hbase::pb::TableName>("test_table");
    unique_ptr <hbase::Table> table = client.Table(tn);

    std::vector <hbase::Get> gets;
    string row_prefix = "rowkey";
    uint64_t num_rows = 2;
    MakeGets(num_rows, row_prefix, gets);

    cout << "before test get" << endl;
    vector <shared_ptr<hbase::Result>> results = table->Get(gets);
    cout << "after  test get" << endl;

    PrintMultiResults(num_rows, results, gets);

    table->Close();
    client.Close();

    return 0;
}
