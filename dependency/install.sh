#!/bin/sh

tar -zxvf dependencies.tar.gz

GROUP=com.gopivotal
VERSION=2.0.2-alpha-gphd-2.0.1.0-SNAPSHOT

mvn install:install-file -Dfile=./hadoop-auth-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=$GROUP -DartifactId=hadoop-auth -Dversion=$VERSION -Dpackaging=jar
mvn install:install-file -Dfile=./hadoop-common-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=$GROUP -DartifactId=hadoop-common -Dversion=$VERSION -Dpackaging=jar
mvn install:install-file -Dfile=./hadoop-hdfs-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=$GROUP -DartifactId=hadoop-hdfs -Dversion=$VERSION -Dpackaging=jar
mvn install:install-file -Dfile=./hadoop-mapreduce-client-common-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=$GROUP -DartifactId=hadoop-mapreduce-client-common -Dversion=$VERSION -Dpackaging=jar
mvn install:install-file -Dfile=./hadoop-mapreduce-client-core-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=$GROUP -DartifactId=hadoop-mapreduce-client-core -Dversion=$VERSION -Dpackaging=jar
mvn install:install-file -Dfile=./hadoop-yarn-api-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=$GROUP -DartifactId=hadoop-yarn-api -Dversion=$VERSION -Dpackaging=jar
mvn install:install-file -Dfile=./hadoop-yarn-common-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=$GROUP -DartifactId=hadoop-yarn-common -Dversion=$VERSION -Dpackaging=jar
mvn install:install-file -Dfile=./gpxf-2.0.0.jar -DgroupId=$GROUP -DartifactId=gpxf -Dversion=2.0.0 -Dpackaging=jar

cd ../gpxf-test
mvn clean install