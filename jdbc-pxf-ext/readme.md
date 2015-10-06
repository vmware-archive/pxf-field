pxf-field / jdbc-pxf-ext
=========

you can build without testing to avoid connection exception
```sh
mvn clean install -DskipTests
```
copy jar file to hawq installation folder and update /etc/gphd/pxf/conf/pxf-public.classpath

add jdbc profile to file /etc/gphd/pxf/conf/pxf-profiles.xml

```xml
<profile>
	<name>JDBC</name>
	<description>A profile for reading JDBC data
	</description>
	<plugins>
		<accessor>com.pivotal.pxf.plugins.jdbc.JdbcAccessor</accessor>
		<resolver>com.pivotal.pxf.plugins.jdbc.JdbcResolver</resolver>
		<fragmenter>com.pivotal.pxf.plugins.jdbc.JdbcFragmenter</fragmenter>
	</plugins>
</profile>
```

restart pxf service
```sh
service pxf-service restart
```

To write rows to terradata via HAWQ:


```sql
DROP EXTERNAL TABLE tr_ext_write;
CREATE WRITABLE EXTERNAL TABLE tr_ext_write(
	Kolumna_1 varchar(20),
	Kolumna_2 varchar(20),
  Kolumna_3 varchar(20)
)location('pxf://hostname:51200/my_path?PROFILE=JDBC&JDBC_DRIVER=com.teradata.jdbc.TeraDriver&DB_URL=jdbc:teradata://TDExpress1403_Sles10&USER=username&PASS=password&TABLE_NAME=schema.tablename&PXF_HOST=hostname')
format 'CUSTOM' (FORMATTER='pxfwritable_export');

insert into tr_ext_write  (Kolumna_1, Kolumna_2, Kolumna_3) values ('ala' ,'mola','dola');
insert into tr_ext_write  (Kolumna_1, Kolumna_2, Kolumna_3) values ('2' ,'3','1');

```

select on terradata side to check
```sql
select * from schema.tablename
where Kolumna_1 = 'ala' and Kolumna_2 = 'mola' and Kolumna_3 = 'dola'
or Kolumna_1 = '2' and Kolumna_2 = '3' and Kolumna_3 = '1';
```
```txt
Kolumna_1                                           Kolumna_2                                           Kolumna_3                                           
------------------------------------------------------------------------------------------------------------------------------------------------------------
2                                                   3                                                   1                                                   
ala                                                 mola                                                dola                                                
```

To read data from terradata via HAWQ:
```sql
DROP EXTERNAL TABLE tr_ext_read;
CREATE EXTERNAL TABLE tr_ext_read(
	Kolumna_1 varchar(20),
	Kolumna_2 varchar(20),
  Kolumna_3 varchar(20)
)location('pxf://hostname:51200/my_path?PROFILE=JDBC&JDBC_DRIVER=com.teradata.jdbc.TeraDriver&DB_URL=jdbc:teradata://TDExpress1403_Sles10&USER=username&PASS=password&TABLE_NAME=schema.tablename&PXF_HOST=hostname')
format 'CUSTOM' (FORMATTER='pxfwritable_import');
```

perform select
```sql
select * from tr_ext_read limit 10;
```


```txt
kolumna_1                                           kolumna_2                                           kolumna_3                                           
------------------------------------------------------------------------------------------------------------------------------------------------------------
 Column_1_12906                                     Column_2_12906                                      Column_3_12906                                      
 Column_1_33922                                     Column_2_33922                                      Column_3_33922                                      
 Column_1_21582                                     Column_2_21582                                      Column_3_21582                                      
 Column_1_44128                                     Column_2_44128                                      Column_3_44128                                      
 Column_1_34212                                     Column_2_34212                                      Column_3_34212                                      
 Column_1_26037                                     Column_2_26037                                      Column_3_26037                                      
 Column_1_44279                                     Column_2_44279                                      Column_3_44279                                      
 Column_1_33045                                     Column_2_33045                                      Column_3_33045                                      
 Column_1_30658                                     Column_2_30658                                      Column_3_30658                                      
 Column_1_28326                                     Column_2_28326                                      Column_3_28326     
```
