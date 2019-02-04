package com.cloudera.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;

public class ExecuteQuery {


	public static void main(String[] args) throws Exception {
		//create();
		verify();
	}

	private static void verify() throws SQLException {
		Connection con = ClouderaImpalaJdbcExample.getConnection();
		Statement stmt = null;
		ResultSet rs;
		int i= 0;
		
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery("show tables;");
			while (rs.next()){
				i++;
				System.out.println(i);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			stmt.close();
			con.close();
		}

	}

	private static void create() throws IOException, SQLException {
		Connection con = ClouderaImpalaJdbcExample.getConnection();
		Statement stmt = null;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		try {
			stmt = con.createStatement();
			fileReader = new FileReader(
					new File(
							"/home/cloudera/Downloads/citi_hive_ddls/hive_ual_rptview_s1_14062018.sql"));
			bufferedReader = new BufferedReader(fileReader);
			StringBuffer stringBuffer = new StringBuffer();
			String line;
			System.out.println("start.......");
			while ((line = bufferedReader.readLine()) != null) {
				if (StringUtils.isNotBlank(line)) {
					try {
						System.out.println("executing : " + line);
						stmt.execute(line);
					} catch (SQLException e) {
						e.printStackTrace();
					}
					stringBuffer.append(line);
				}
			}

			System.out.println("end...");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			bufferedReader = null;
			fileReader.close();
			stmt.close();
			con.close();
		}
	}
}
