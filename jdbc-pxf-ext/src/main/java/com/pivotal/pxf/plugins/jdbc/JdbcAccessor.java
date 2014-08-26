package com.pivotal.pxf.plugins.jdbc;

import java.io.IOException;
import java.sql.*;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.WriteAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class JdbcAccessor extends Plugin implements WriteAccessor {

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

	public JdbcAccessor(InputData input) throws IOException {
		super(input);

		jdbcDriver = input.getParametersMap().get("X-GP-JDBC_DRIVER");
		dbUrl = input.getParametersMap().get("X-GP-DB_URL");
		user = input.getParametersMap().get("X-GP-USER");
		pass = input.getParametersMap().get("X-GP-PASS");
		String strBatch = input.getParametersMap().get("X-GP-BATCH_SIZE");

		if (strBatch != null) {
			batchSize = Integer.parseInt(strBatch);
		}

		if (jdbcDriver == null) {
			throw new IllegalArgumentException("JDBC_DRIVER must be set");
		}

		if (dbUrl == null) {
			throw new IllegalArgumentException("DB_URL must be set");
		}

		tblName = input.getParametersMap().get("X-GP-TABLE_NAME");

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

		if (tblName.contains(".")) {
			String schema = tblName.split("\\.")[0];
			String table = tblName.split("\\.")[1];

			statement.execute("USE " + schema);
			ResultSet tables = statement.executeQuery("SHOW TABLES");

			boolean found = false;
			while (tables.next()) {
				if (tables.getString(1).equals(table)) {
					found = true;
					break;
				}
			}

			if (!found) {
				throw new SQLException("Table " + tblName + " not found.");
			}
		} else {
			ResultSet tables = statement.executeQuery("SHOW TABLES");

			boolean found = false;
			while (tables.next()) {
				if (tables.getString(1).equals(tblName)) {
					found = true;
					break;
				}
			}

			if (!found) {
				throw new SQLException("Table " + tblName + " not found.");
			}
		}

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
}
