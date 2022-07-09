cmake_minimum_required(VERSION 3.0)
project(libhbase_wrapper LANGUAGES CXX VERSION 1.0.0)

set(CMAKE_C_COMPILER /opt/rh/devtoolset-7/root/bin/gcc)
set(CMAKE_CXX_COMPILER /opt/rh/devtoolset-7/root/bin/g++)
set(CMAKE_CXX_STANDARD 11)
set(D_GLIBCXX_USE_CXX11_ABI 0)

set(CMAKE_CXX_FLAGS "-std=c++0x ${CMAKE_CXX_FLAGS} -g -ftest-coverage -fprofile-arcs")

include_directories(
  $ENV{JAVA_HOME}/include
  $ENV{JAVA_HOME}/include/linux
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

FIND_LIBRARY(HBASE_LIBS NAMES libhbase.so PATHS ${CMAKE_CURRENT_SOURCE_DIR}/lib/so)
FIND_LIBRARY(JVM_LIBS NAMES libjvm.so PATHS $ENV{JAVA_HOME}/jre/lib/amd64/server)
FIND_LIBRARY(BYTE_BUFFER_LIBS NAMES libbytebuffer.so PATHS ${CMAKE_CURRENT_SOURCE_DIR}/lib/so)

add_executable(libhbase_wrapper src/native_client_wrapper.cpp)
add_library(hbaseclient STATIC src/native_client_wrapper.cpp)

#install(TARGETS libhbaseclient DESTINATION /usr/local/lib64)
#install(FILES include/native_client_wrapper.hpp DESTINATION include)

target_link_libraries(libhbase_wrapper PRIVATE hbaseclient ${HBASE_LIBS} ${BYTE_BUFFER_LIBS} ${JVM_LIBS})
