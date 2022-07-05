cmake_minimum_required(VERSION 3.22)
project(hbase-native-client-test LANGUAGES CXX VERSION 1.0.0)

set(CMAKE_C_COMPILER /opt/rh/devtoolset-7/root/bin/gcc)
set(CMAKE_CXX_COMPILER /opt/rh/devtoolset-7/root/bin/g++)
#set(CMAKE_CXX_STANDARD 11)
set(D_GLIBCXX_USE_CXX11_ABI 0)

include_directories(
        /usr/local/include/hbase/
        /home/ddadmin/Downloads/hbase-native-client/build/CMakeFiles
        /home/ddadmin/Downloads/hbase-native-client/build/CMakeFiles/hbase/if
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/boost-install/include
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/boost-install/lib
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/facebook-folly-proj-install/include
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/Protobuf-build/include
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/facebook-wangle-proj-install/include
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/zookeeper-install/include
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/facebook-folly-proj-install/lib/
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/facebook-folly-proj-install/include/folly/
        /usr/lib/jvm/java-1.8.0-openjdk/include
        /usr/lib/jvm/java-1.8.0-openjdk/include/linux
        /usr/local/lib64/
        /home/ddadmin/Downloads/hbase-native-client/build/dependencies/Protobuf-src/gmock/gtest/include/
)

FIND_LIBRARY(HBASE_LIBS NAMES libhbaseclient.so)
FIND_LIBRARY(FOLLY_LIBS NAMES libfolly.so.57)

add_executable(hbase-native-client-test src/main.cpp)

target_link_libraries(hbase-native-client-test -Lhbaseclient ${HBASE_LIBS} -Lfolly ${FOLLY_LIBS})