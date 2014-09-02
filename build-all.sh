#!/bin/sh


rm -f build/*.jar

cd pxf-test
mvn clean install
cp target/*.jar ../build
cd ..

mkdir -p build

for dir in `ls | grep -ext`; do
	cd $dir
	mvn clean package
	cp target/*.jar ../build
	cd ..
done

exit 0
