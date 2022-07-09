#! /bin/bash
rm -rf  CMakeFiles/ CMakeCache.txt cmake_install.cmake libhbase_wrapper liblibhbaseclient.so Makefile
cmake .. && make
# for libhbase
# export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HOME}/libhbase/lib/so:${JAVA_HOME}/jre/lib/amd64/server
# export HBASE_CONF_DIR=/etc/hbase/conf
# export CLASSPATH=${CLASSPATH}:$(echo /usr/hdp/current/hbase-client/lib/* | tr ' ' ':'):/usr/hdp/current/hadoop-client/hadoop-common.jar:/usr/hdp/current/hadoop-client/hadoop-auth.jar:$(echo ${HOME}/libhbase/lib/jar/* | tr ' ' ':')
