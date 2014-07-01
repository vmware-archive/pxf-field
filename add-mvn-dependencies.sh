#!/bin/sh

# The default behavior of this script is to search the default PHD installation locations for JAR files.  Declare a local lib directory to look there instead.  The lib directory should only contain jar files, not any sub-directories
#LIB_DIR=lib

PXF_VERSION=2.2.0.0

if [[ -z $LIB_DIR ]]; then
    PXF_ROOT=/usr/lib/gphd/pxf
else
    PXF_ROOT=/$LIB_DIR
fi

PACKAGING=jar

mvn install:install-file -Dfile=$PXF_ROOT/pxf-api-$PXF_VERSION.jar -DgroupId=com.gopivotal -DartifactId=pxf-api -Dversion=$PXF_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $PXF_ROOT/pxf-api-$PXF_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$PXF_ROOT/pxf-core-$PXF_VERSION.jar -DgroupId=com.gopivotal -DartifactId=pxf-core -Dversion=$PXF_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $PXF_ROOT/pxf-core-$PXF_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$PXF_ROOT/pxf-hbase-$PXF_VERSION.jar -DgroupId=com.gopivotal -DartifactId=pxf-hbase -Dversion=$PXF_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $PXF_ROOT/pxf-hbase-$PXF_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$PXF_ROOT/pxf-hdfs-$PXF_VERSION.jar -DgroupId=com.gopivotal -DartifactId=pxf-hdfs -Dversion=$PXF_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $PXF_ROOT/pxf-hdfs-$PXF_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

mvn install:install-file -Dfile=$PXF_ROOT/pxf-hive-$PXF_VERSION.jar -DgroupId=com.gopivotal -DartifactId=pxf-hive -Dversion=$PXF_VERSION -Dpackaging=jar &> /dev/null
if [[ $? -ne 0 ]]; then echo "Failed to install $PXF_ROOT/pxf-hive-$PXF_VERSION.jar... Does it exist?  Exiting."; exit 1; fi

echo "All dependencies added successfully."

exit 0

