cmake_minimum_required(VERSION 3.0)
project(libhbase_wrapper LANGUAGES CXX VERSION 1.0.0)
# cmake_minimum_required(VERSION 2.8)
# project(libhbase_wrapper)

set(CMAKE_C_COMPILER /opt/rh/devtoolset-7/root/bin/gcc)
set(CMAKE_CXX_COMPILER /opt/rh/devtoolset-7/root/bin/g++)
# set(CMAKE_C_COMPILER /bin/gcc)
# set(CMAKE_CXX_COMPILER /bin/g++)
set(CMAKE_CXX_STANDARD 11)
set(D_GLIBCXX_USE_CXX11_ABI 0)

#set(CMAKE_CXX_FLAGS "-std=c++0x ${CMAKE_CXX_FLAGS} -g -ftest-coverage -fprofile-arcs -fpermissive")
set(CMAKE_CXX_FLAGS "-std=c++0x ${CMAKE_CXX_FLAGS} -g -ftest-coverage -fprofile-arcs")

include_directories(
  /home/ddadmin/Downloads/libhbase-dd/src/main/native/include
  /home/ddadmin/Downloads/libhbase-dd/src/test/native/common
  /home/ddadmin/Downloads/libhbase-dd/target/gtest-1.7.0/include
  /usr/lib/jvm/java-1.8.0-openjdk/include
  /usr/lib/jvm/java-1.8.0-openjdk/include/linux
  /usr/local/lib64
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

FIND_LIBRARY(GTEST_LIBS NAMES libgtest.so PATHS /home/ddadmin/Downloads/libhbase-dd/target/gtest-1.7.0)
FIND_LIBRARY(HBASE_LIBS NAMES libhbase.so PATHS /home/ddadmin/Downloads/libhbase-dd/target/libhbase-1.0-SNAPSHOT/lib/native)
FIND_LIBRARY(JVM_LIBS NAMES libjvm.so PATHS /usr/lib/jvm/java-1.8.0-openjdk/jre/lib/amd64/server)
FIND_LIBRARY(BYTE_BUFFER_LIBS NAMES libbytebuffer.so PATHS /home/ddadmin/Downloads/libhbase-dd/src/test/native/common)

#add_executable(libhbase_wrapper_example src/main.cpp)
add_executable(libhbase_wrapper src/native_client_wrapper.cpp)
#add_library(libhbaseclient SHARED src/native_client_wrapper.cpp)
add_library(hbaseclient STATIC src/native_client_wrapper.cpp)

#install(TARGETS libhbaseclient DESTINATION /usr/local/lib64)
#install(FILES include/native_client_wrapper.hpp DESTINATION include)

#target_link_libraries(libhbase_wrapper LINK_PUBLIC ${GTEST_LIBS} ${HBASE_LIBS} ${JVM_LIBS} ${BYTE_BUFFER_LIBS})
#target_link_libraries(libhbase_wrapper_example LINK_PUBLIC ${GTEST_LIBS} ${HBASE_LIBS} ${JVM_LIBS} ${BYTE_BUFFER_LIBS})
#target_link_libraries(libhbase_wrapper LINK_PUBLIC ${HBASE_LIBS} ${JVM_LIBS} ${BYTE_BUFFER_LIBS})
target_link_libraries(libhbase_wrapper PUBLIC hbaseclient ${HBASE_LIBS} ${BYTE_BUFFER_LIBS} ${JVM_LIBS})
