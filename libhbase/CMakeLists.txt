cmake_minimum_required(VERSION 2.8)
project(libhbase_wrapper)

set(CMAKE_C_COMPILER /bin/gcc)
set(CMAKE_CXX_COMPILER /bin/g++)
set(CMAKE_CXX_STANDARD 11)
set(D_GLIBCXX_USE_CXX11_ABI 0)

set(CMAKE_CXX_FLAGS "-std=c++0x ${CMAKE_CXX_FLAGS} -g -ftest-coverage -fprofile-arcs")

include_directories(
  /home/ddadmin/Downloads/libhbase/src/main/native/include
  /home/ddadmin/Downloads/libhbase/src/test/native/common
  /home/ddadmin/Downloads/libhbase/target/gtest-1.7.0/include
  /usr/local/lib64
)

FIND_LIBRARY(GTEST_LIBS NAMES libgtest.so PATHS /home/ddadmin/Downloads/libhbase/target/gtest-1.7.0)
FIND_LIBRARY(HBASE_LIBS NAMES libhbase.so PATHS /home/ddadmin/Downloads/libhbase/target/libhbase-1.0-SNAPSHOT/lib/native)
FIND_LIBRARY(JVM_LIBS NAMES libjvm.so PATHS /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/amd64/server)
FIND_LIBRARY(BYTE_BUFFER_LIBS NAMES libbytebuffer.so PATHS /home/ddadmin/Downloads/libhbase/src/test/native/common)

# add_executable(hbase-native-client-test src/main.cpp)
set_target_properties(libhbase_wrapper PROPERTIES SOVERSION 1)

target_link_libraries(hbase-native-client-test LINK_PUBLIC ${GTEST_LIBS} ${HBASE_LIBS} ${JVM_LIBS} ${BYTE_BUFFER_LIBS})
