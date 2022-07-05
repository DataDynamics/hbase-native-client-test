#! /bin/bash
rm -rf * && cmake .. && make
export CLASSPATH=$(echo /home/ddadmin/Downloads/hbase-libs-hdp/* | tr ' ' ':') ; cls ; ./hbase-native-client-test
