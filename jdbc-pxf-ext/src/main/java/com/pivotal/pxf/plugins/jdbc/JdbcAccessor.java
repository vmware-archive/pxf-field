package com.pivotal.pxf.plugins.jdbc;

import java.io.IOException;
import java.lang.Object;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.WriteAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import org.omg.CORBA.*;

public class JdbcAccessor extends Plugin implements WriteAccessor,ReadAccessor {

	// private static final Logger LOG = Logger.getLogger(JdbcAccessor.class);
	private String jdbcDriver = null;
	private String dbUrl = null;
	private String user = null;
	private String pass = null;
	private String tblName = null;
	private Connection conn = null;
	private Statement statement = null;
	private int batchSize = 100;
	private int numAdded = 0;

	private ResultSet rowsFromTable;
	private OneRow oneRow = new OneRow();

	public JdbcAccessor(InputData input) throws IOException {
		super(input);

		jdbcDriver = input.getUserProperty("JDBC_DRIVER");
		dbUrl = input.getUserProperty("DB_URL");
		user = input.getUserProperty("USER");
		pass = input.getUserProperty("PASS");
		String strBatch = input.getUserProperty("BATCH_SIZE");

		if (strBatch != null) {
			batchSize = Integer.parseInt(strBatch);
		}

		if (jdbcDriver == null) {
			throw new IllegalArgumentException("JDBC_DRIVER must be set");
		}

		if (dbUrl == null) {
			throw new IllegalArgumentException("DB_URL must be set");
		}

		tblName = input.getUserProperty("TABLE_NAME");

		if (tblName == null) {
			throw new IllegalArgumentException("TABLE_NAME must be set");
		} else {
			tblName = tblName.toUpperCase();
		}
	}

	@Override
	public boolean openForWrite() throws Exception {
		Class.forName(jdbcDriver);

		if (user != null) {
			conn = DriverManager.getConnection(dbUrl, user, pass);
		} else {
			conn = DriverManager.getConnection(dbUrl);
		}

		conn.setAutoCommit(false);

		statement = conn.createStatement();

		return true;
	}

	@Override
	public boolean writeNextObject(OneRow row) throws Exception {

		statement.addBatch(row.getData().toString());
		++numAdded;

		if (numAdded % batchSize == 0) {
			statement.executeBatch();
			numAdded = 0;
		}

		return true;
	}

	@Override
	public void closeForWrite() throws Exception {

		if (statement != null && numAdded != 0) {
			statement.executeBatch();
			numAdded = 0;
		}

		if (conn != null) {
			conn.commit();
			conn.close();
		}
	}

	@Override
	public boolean openForRead() throws Exception {
		Class.forName(jdbcDriver);

		if (user != null) {
			conn = DriverManager.getConnection(dbUrl, user, pass);
		} else {
			conn = DriverManager.getConnection(dbUrl);
		}

		conn.setAutoCommit(false);

		statement = conn.createStatement();

		rowsFromTable = statement.executeQuery("SELECT * FROM " + tblName); // read all rows from table

		return true;
	}

	@Override
	public OneRow readNextObject() throws Exception {

		if(rowsFromTable.next()){
			oneRow.setKey(rowsFromTable.getRow());

			int col_count = rowsFromTable.getMetaData().getColumnCount();
			List<HashMap<String, Object>> row = new ArrayList<HashMap<String, Object>>();

			for(int i=1;i<=col_count;i++){
				HashMap<String,Object> col = new HashMap<String,Object>();
				col.put(rowsFromTable.getMetaData().getColumnTypeName(i),rowsFromTable.getObject(i));
				row.add(col);
			}

			oneRow.setData(row);

			return oneRow;

		}else{

			return null;
		}
	}

	@Override
	public void closeForRead() throws Exception {
		if (conn != null) {
			conn.close();
		}
	}
}
