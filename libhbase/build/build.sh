#! /bin/bash
rm -rf  CMakeFiles/ CMakeCache.txt cmake_install.cmake libhbase_wrapper liblibhbaseclient.so Makefile
cmake .. && make
# export HBASE_CONF_DIR=/etc/hbase/conf
# export CLASSPATH=$(echo /home/ddadmin/Downloads/hbase-libs-hdp/* | tr ' ' ':')
# ./libhbase_wrapper
