package com.cloudera.example;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ClouderaImpalaJdbcExample {

	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";

	private static String connectionUrl;
	private static String jdbcDriverName;

	private static void loadConfiguration() throws IOException {
		InputStream input = null;
		try {
			String filename = ClouderaImpalaJdbcExample.class.getSimpleName()
					+ ".conf";
			System.out.println("Configuration file: " + filename);
			input = ClouderaImpalaJdbcExample.class.getClassLoader()
					.getResourceAsStream(filename);
			Properties prop = new Properties();
			prop.load(input);

			connectionUrl = prop.getProperty(CONNECTION_URL_PROPERTY);
			jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
		} finally {
			try {
				if (input != null)
					input.close();
			} catch (IOException e) {
				// nothing to do
			}
		}
	}

	public static Connection getConnection() {
		connectionUrl = "jdbc:hive2://localhost:21050/singh;auth=noSasl";
		jdbcDriverName = "org.apache.hive.jdbc.HiveDriver";

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + connectionUrl);
		System.out.println("USing JDBC Driver " + jdbcDriverName);
		Connection con = null;
		try {

			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return con;
	}

	public static void main(String[] args) throws IOException {

		/*
		 * if (args.length != 1) {
		 * System.out.println("Syntax: ClouderaImpalaJdbcExample \"<SQL_query>\""
		 * ); System.exit(1); }
		 */

		loadConfiguration();

		String sqlStatement = "select * from t1";
		System.out.println("Running Query: " + sqlStatement);

		Connection con = getConnection();

		try {

			Statement stmt = con.createStatement();

			ResultSet rs = stmt.executeQuery(sqlStatement);

			System.out
					.println("\n== Begin Query Results ======================");

			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				System.out.println(rs.getString(1));
			}

			System.out
					.println("== End Query Results =======================\n\n");

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
			}
		}
	}
}
