#!/bin/sh

tar -zxvf dependencies.tar.gz

GROUP=com.gopivotal
VERSION=2.0.2-alpha-gphd-2.0.1.0

mvn install:install-file -Dfile=./gpxf-2.0.1.jar -DgroupId=$GROUP -DartifactId=gpxf -Dversion=2.0.1 -Dpackaging=jar

cd ../pxf-test
mvn clean install
