#!/bin/sh

tar -zxvf dependencies.tar.gz

GROUP=com.gopivotal

mvn install:install-file -Dfile=./pxf-2.1.0.jar -DgroupId=$GROUP -DartifactId=pxf -Dversion=2.1.0 -Dpackaging=jar

rm pxf-2.1.0.jar

cd ../pxf-test
mvn clean install

exit 0
