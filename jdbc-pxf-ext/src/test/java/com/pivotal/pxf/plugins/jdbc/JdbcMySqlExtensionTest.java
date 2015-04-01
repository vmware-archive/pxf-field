package com.pivotal.pxf.plugins.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.pivotal.pxf.PxfUnit;
import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.WriteAccessor;
import com.pivotal.pxf.api.WriteResolver;
import com.pivotal.pxf.api.io.DataType;

public class JdbcMySqlExtensionTest extends PxfUnit {

	private JdbcAccessor accessor = null;
	private JdbcResolver resolver = null;
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DATABASE = "PXFTEST";
	private static final String JDBC_CONNECTION = "jdbc:mysql://pcc:3306";
	private static final String DB_URL = JDBC_CONNECTION + "/" + DATABASE;
	private static final String TABLE_NAME = "PXFTEST";
	private static final String POWER_USER = "root";
	private static final String POWER_USER_PASSWORD = "secret";
	private static final String USER = "pxfuser";
	private static final String PASS = "secret";

	private static List<Pair<String, DataType>> columnDefs = new ArrayList<Pair<String, DataType>>();
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	@Before
	public void setup() throws Exception {
		extraParams.add(new Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
		extraParams.add(new Pair<String, String>("DB_URL", DB_URL));
		extraParams.add(new Pair<String, String>("TABLE_NAME", TABLE_NAME));
		extraParams.add(new Pair<String, String>("USER", USER));
		extraParams.add(new Pair<String, String>("PASS", PASS));

		Connection conn = DriverManager.getConnection(JDBC_CONNECTION,
				POWER_USER, POWER_USER_PASSWORD);
		Statement stmnt = conn.createStatement();

		stmnt.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
		stmnt.execute("USE " + DATABASE);
	}

	private void createUser() throws SQLException {
		createUser(true);
	}

	private void createUser(boolean withPass) throws SQLException {
		Connection conn = DriverManager.getConnection(JDBC_CONNECTION,
				POWER_USER, POWER_USER_PASSWORD);
		Statement stmnt = conn.createStatement();

		if (withPass) {
			stmnt.execute("CREATE USER '" + USER + "' IDENTIFIED BY '" + PASS
					+ "'");
		} else {
			stmnt.execute("CREATE USER '" + USER + "'");
		}

		stmnt.execute("USE " + DATABASE);
		stmnt.execute("GRANT ALL PRIVILEGES ON " + DATABASE + " TO '" + USER
				+ "'");
	}

	@After
	public void cleanup() throws Exception {
		extraParams.clear();

		Connection conn = DriverManager.getConnection(JDBC_CONNECTION,
				POWER_USER, POWER_USER_PASSWORD);
		Statement stmnt = conn.createStatement();

		try {
			stmnt.execute("DROP USER '" + USER + "'@'%'");
		} catch (SQLException e) {
		}

		stmnt.execute("DROP TABLE IF EXISTS " + DATABASE + "." + TABLE_NAME);
		stmnt.execute("DROP DATABASE IF EXISTS " + DATABASE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNoJdbcDriver() throws IOException {
		columnDefs.add(new Pair<String, DataType>("foo", DataType.BIGINT));

		extraParams.clear();
		extraParams.add(new Pair<String, String>("DB_URL", DB_URL));
		extraParams.add(new Pair<String, String>("TABLE_NAME", TABLE_NAME));

		accessor = new JdbcAccessor(super.getInputDataForWritableTable());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNoDBUrl() throws IOException {
		columnDefs.add(new Pair<String, DataType>("foo", DataType.BIGINT));

		extraParams.clear();

		extraParams.add(new Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
		extraParams.add(new Pair<String, String>("TABLE_NAME", TABLE_NAME));

		accessor = new JdbcAccessor(super.getInputDataForWritableTable());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNoTable() throws IOException {
		columnDefs.add(new Pair<String, DataType>("foo", DataType.BIGINT));

		extraParams.clear();

		extraParams.add(new Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
		extraParams.add(new Pair<String, String>("DB_URL", DB_URL));

		accessor = new JdbcAccessor(super.getInputDataForWritableTable());
	}

	@Test(expected = SQLException.class)
	public void testNoUserButRequired() throws Exception {
		columnDefs.add(new Pair<String, DataType>("foo", DataType.BIGINT));

		extraParams.clear();

		extraParams.add(new Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
		extraParams.add(new Pair<String, String>("DB_URL", DB_URL));
		extraParams.add(new Pair<String, String>("TABLE_NAME", TABLE_NAME));

		accessor = new JdbcAccessor(super.getInputDataForWritableTable());
		accessor.openForWrite();
	}

	@Test(expected = SQLException.class)
	public void testNoPasswordNotRequired() throws Exception {

		createUser(false);

		columnDefs.add(new Pair<String, DataType>("foo", DataType.BIGINT));

		extraParams.clear();

		extraParams.add(new Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
		extraParams.add(new Pair<String, String>("DB_URL", DB_URL));
		extraParams.add(new Pair<String, String>("TABLE_NAME", TABLE_NAME));
		extraParams.add(new Pair<String, String>("USER", USER));

		accessor = new JdbcAccessor(super.getInputDataForWritableTable());
		accessor.openForWrite();
	}

	@Test(expected = SQLException.class)
	public void testNoPasswordButRequired() throws Exception {

		createUser();

		columnDefs.add(new Pair<String, DataType>("foo", DataType.BIGINT));

		extraParams.clear();

		extraParams.add(new Pair<String, String>("JDBC_DRIVER", JDBC_DRIVER));
		extraParams.add(new Pair<String, String>("DB_URL", DB_URL));
		extraParams.add(new Pair<String, String>("TABLE_NAME", TABLE_NAME));
		extraParams.add(new Pair<String, String>("USER", USER));

		accessor = new JdbcAccessor(super.getInputDataForWritableTable());
		accessor.openForWrite();
	}

	@Test
	public void testBigInt() throws Exception {

		createUser();

		// Create table
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);

		Statement stmnt = conn.createStatement();
		stmnt.execute("CREATE TABLE " + TABLE_NAME + " (A BIGINT)");

		// Test extension
		columnDefs.add(new Pair<String, DataType>("A", DataType.BIGINT));

		accessor = new JdbcAccessor(super.getInputDataForWritableTable());
		resolver = new JdbcResolver(super.getInputDataForWritableTable());

		List<String> allRows = new ArrayList<String>();

		List<OneField> fields = new ArrayList<OneField>();
		List<OneRow> rows = new ArrayList<OneRow>();
		fields.add(new OneField(DataType.BIGINT.getOID(), Long.MIN_VALUE));

		allRows.add(Long.toString(Long.MIN_VALUE));

		Assert.assertTrue("Failed to open accessor for write",
				accessor.openForWrite());

		accessor.writeNextObject(resolver.setFields(fields));

		fields.clear();
		fields.add(new OneField(DataType.BIGINT.getOID(), Long.MAX_VALUE));

		allRows.add(Long.toString(Long.MAX_VALUE));
		accessor.writeNextObject(resolver.setFields(fields));

		accessor.closeForWrite();

		// Validate Data
		ResultSet rs = stmnt.executeQuery("SELECT * FROM " + TABLE_NAME);

		int i = 0;
		while (rs.next()) {
			Assert.assertEquals(allRows.get(i++), rs.getString(1));
		}
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}

	@Override
	public Class<? extends WriteAccessor> getWriteAccessorClass() {
		return JdbcAccessor.class;
	}

	@Override
	public Class<? extends WriteResolver> getWriteResolverClass() {
		return JdbcResolver.class;
	}
}
