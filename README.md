pxf-field
=========

All of these projects reference various Pivotal HD libraries using Maven.  These libraries are largely in the Spring Source repository:

```xml
<repository>
   <id>spring-releases</id>
   <name>Spring Release Repository</name>
   <url>https://repo.spring.io/libs-release</url>
   <releases>
     <enabled>true</enabled>
   </releases>
   <snapshots>
     <enabled>false</enabled>
   </snapshots>
</repository>
```

This repository is referenced in each of the PXF projects. 

In addition to resolve the PXF dependecies you should use the [Big-Data Maven Repository](https://bintray.com/big-data/maven/pxf/view):
```xml
<repository>
  <id>bintray</id>
  <name>HAWQ-MR/PXF Release Repository</name>
  <url>https://dl.bintray.com/big-data/maven</url>
  <releases>
    <enabled>true</enabled>
  </releases>
  <snapshots>
    <enabled>false</enabled>
  </snapshots>  
</repository>
```
> Note: the com.`gopivotal` group name is renamed to com.`pivotal`.

The PXF-2.5.1.0 dependencies:
```xml
<dependency>
  <groupId>com.pivotal</groupId>
  <artifactId>pxf-api</artifactId>
  <version>2.5.1.0</version>  
</dependency>

<dependency>
  <groupId>com.pivotal</groupId> 
  <artifactId>pxf-service</artifactId>
  <version>2.5.1.0</version>
</dependency>

<dependency>
  <groupId>com.pivotal</groupId>
  <artifactId>pxf-hbase</artifactId>
  <version>2.5.1.0</version>
</dependency>

<dependency>
  <groupId>com.pivotal</groupId>
  <artifactId>pxf-hdfs</artifactId>
  <version>2.5.1.0</version>
</dependency>

<dependency>
  <groupId>com.pivotal</groupId>
  <artifactId>pxf-hive</artifactId>
  <version>2.5.1.0</version>
</dependency>
```

Note that the same repository serves pre-build pxf-field jars as well:

```xml
<dependency>
   <groupId>com.gopivotal</groupId>
   <artifactId>accumulo-pxf-ext</artifactId>
   <version>3.0.0.0-18</version>
</dependency>

<dependency>
   <groupId>com.gopivotal</groupId>
   <artifactId>cassandra-pxf-ext</artifactId>
   <version>3.0.0.0-18</version>
</dependency>

<dependency>
  <groupId>com.gopivotal</groupId>
  <artifactId>hive-hotfix-pxf-ext</artifactId>
  <version>3.0.0.0-18</version>
</dependency>

<dependency>
  <groupId>com.gopivotal</groupId>
  <artifactId>jdbc-pxf-ext</artifactId>
  <version>3.0.0.0-18</version>
</dependency>

<dependency>
  <groupId>com.gopivotal</groupId>
  <artifactId>json-pxf-ext</artifactId>
  <version>3.0.0.0-18</version>
</dependency>

<dependency>
  <groupId>com.gopivotal</groupId>
  <artifactId>pxf-pipes</artifactId>
  <version>3.0.0.0-18</version>
</dependency>

<dependency>
  <groupId>com.gopivotal</groupId>
  <artifactId>pxf-test</artifactId>
  <version>3.0.0.0-18</version>
</dependency>

<dependency>
  <groupId>com.gopivotal</groupId>
  <artifactId>redis-pxf-ext</artifactId>
  <version>3.0.0.0-18</version>
</dependency>
```



To build the dependecies yourself youThe dependency directory contains an install script and tarball of the required libraries.  Execute the install.sh script to install them and build the pxf-test library.

Documentation on these extensions is maintained at http://pivotal-field-engineering.github.io/pxf-field/index.html


