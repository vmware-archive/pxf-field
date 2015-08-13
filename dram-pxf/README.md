
drop external table dram;
CREATE EXTERNAL TABLE dram ( key TEXT, serial TEXT, bits TEXT )
LOCATION ('pxf://phd1.localdomain:51200/dramdata/sampledata/*?Profile=Dram')
FORMAT 'custom' (Formatter='pxfwritable_import');



CREATE EXTERNAL TABLE dummy_tbl (int1 integer, word text, int2 integer)
location
('pxf://localhost:51200/dummy_location?FRAGMENTER=com.pivotal.pxf.plugins.dummy.DummyFragmenter&ACCESSOR=com.pivotal.pxf.plugins.dummy.DummyAccessor&RESOLVER=com.pivotal.pxf.plugins.dummy.DummyResolver&ANALYZER=com.pivotal.pxf.plugins.dummy.DummyAnalyzer')
 format 'custom' (formatter = 'pxfwritable_import');


 CREATE WRITABLE EXTERNAL TABLE dummy_tbl_write (int1 integer, word text, int2 integer)
 location
 ('pxf://localhost:51200/dummy_location?ACCESSOR=com.pivotal.pxf.plugins.dummy.DummyAccessor&RESOLVER=com.pivotal.pxf.plugins.dummy.DummyResolver')
 format 'custom' (formatter = 'pxfwritable_export');
 　