package com.pivotal.pxf.plugins.jdbc;

import java.io.IOException;
import java.util.*;

import com.pivotal.pxf.api.ReadResolver;
import org.apache.log4j.Logger;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.WriteResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class JdbcResolver extends Plugin implements WriteResolver,ReadResolver {

	private static final Logger LOG = Logger.getLogger(JdbcResolver.class);
	private StringBuilder bldr = new StringBuilder();
	private OneRow row = new OneRow();
	private final String INSERT_INTO = "INSERT INTO ";
	private final String TABLE_NAME;
	private final String VALUES = " VALUES (";
	private final String COMMA = ",";
	private final String TICK = "'";
	private final String CLOSE_STMT = ");";
	private ArrayList<OneField> fields = new ArrayList<OneField>();

	public JdbcResolver(InputData input) {
		super(input);
		TABLE_NAME = input.getUserProperty("TABLE_NAME");
	}

	@Override
	public List<OneField> getFields(OneRow oneRow) throws Exception {
		fields.clear();

		List<HashMap<String, Object>> row = (List<HashMap<String, Object>>)oneRow.getData();

		for (HashMap<String, Object> col : row) {
			Iterator it = col.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				OneField oneField = new OneField(getTypeFromColName(pair.getKey().toString()),pair.getValue());

				it.remove(); // avoids a ConcurrentModificationException
				fields.add(oneField);
				LOG.info(oneField.val);
				//LOG.info(oneField.type);
			}

		}

		return fields;
	}

	private int getTypeFromColName(String columnTypeName){
/*		based on following assigment JDBC datatypes and pxf datatypes
		PXF	SQL
		BOOLEAN(16),	BOOLEAN(16)
		BYTEA(17),	VARBINARY(-3)
		CHAR(18),	CHAR(1)
		BIGINT(20),	BIGINT(-5)
		SMALLINT(21),	SMALLINT(5)
		INTEGER(23),	INTEGER(4)
		TEXT(25),	LONGNVARCHAR(-16)
		REAL(700),	REAL(7)
		FLOAT8(701),	FLOAT(6)
		BPCHAR(1042),	CHAR(1)
		VARCHAR(1043),	VARCHAR(12)
		DATE(1082),	DATE(91)
		TIME(1083),	TIME(92)
		TIMESTAMP(1114),	TIMESTAMP(93)
		NUMERIC(1700),	NUMERIC(2)
		UNSUPPORTED_TYPE(-1);	()*/


		switch (columnTypeName){
			case "BOOLEAN" : return 16;
			case "VARBINARY" : return 17;
			case "CHAR" : return 18;
			case "BIGINT" : return 20;
			case "SMALLINT" : return 21;
			case "INTEGER" : return 23;
			case "LONGNVARCHAR" : return 25;
			case "REAL" : return 700;
			case "FLOAT" : return 701;
			case "VARCHAR" : return 1043;
			case "DATE" : return 1082;
			case "TIME" : return 1083;
			case "TIMESTAMP" : return 1114;
			case "NUMERIC" : return 1700;

			default: return -1; // UNSUPPORTED_TYPE(-1);
		}




	}


	@Override
	public OneRow setFields(List<OneField> records) throws Exception {
		LOG.info("SETFIELDS");
		bldr.setLength(0);

		bldr.append(INSERT_INTO);
		bldr.append(TABLE_NAME);
		bldr.append(VALUES);

		int i = 0;
		int length = records.size();
		for (OneField f : records) {
			switch (DataType.get(f.type)) {
				case BIGINT:
				case BOOLEAN:
				case BPCHAR:
				case BYTEA:
				case DATE:
				case FLOAT8:
				case INTEGER:
				case NUMERIC:
				case REAL:
				case SMALLINT:
				case TIME:
				case TIMESTAMP:
					bldr.append(f.val.toString());
					break;
				case CHAR:
				case TEXT:
				case VARCHAR:
					bldr.append(TICK);
					bldr.append(f.val.toString().replaceAll("'", "''"));
					bldr.append(TICK);
					break;
				case UNSUPPORTED_TYPE:
				default:
					throw new IOException("Unsupported type " + f.type);
			}

			if (++i < length) {
				bldr.append(COMMA);
			}
		}

		bldr.append(CLOSE_STMT);

		row.setData(bldr.toString());
		LOG.info("STATEMENT: " + bldr.toString());
		return row;
	}

}
