package com.cloudera.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class InsertRecord {
	public static void main(String[] args) throws IOException, SQLException {
		List<String> quries = readFile();
		List<String> errorList = null;
		Connection con = ClouderaImpalaJdbcExample.getConnection();
		Statement stmt = null;
		StringBuilder stb = null;
		try {
			stmt = con.createStatement();
			do {
				if (null != errorList && errorList.size() > 0) {
					quries = errorList;
				}
				errorList = new ArrayList<String>(0);
				stb = new StringBuilder();
				System.out.println("Query List size : " + quries.size());
				for (String query : quries) {
					try {
						stmt.execute(query);
					} catch (Exception ex) {
						System.err.println(query+"\n\n");
						stb.append(query+"\n\n");
						errorList.add(query);
					}
				}
				System.out.println("Error List size : " + errorList.size());
				System.out.println("====================");
				System.out.println(stb);
				System.out.println("====================");
			} while (errorList.size() != quries.size());

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			stmt.close();
			con.close();
		}

	}

	private static List<String> readFile() throws IOException {
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		List<String> queries = new ArrayList<>();
		try {
			fileReader = new FileReader(new File(
					"/home/cloudera/Downloads/hive_ual_rptview_s1_18062018.sql"));
			bufferedReader = new BufferedReader(fileReader);
			StringBuffer stringBuffer = new StringBuffer();
			String line;
			System.out.println("start.......");
			while ((line = bufferedReader.readLine()) != null) {
				if (StringUtils.isNotBlank(line)) {
					stringBuffer.append(line);
					if (line.contains("ual_rptview_s1")) {
						line = line.replaceAll("ual_rptview_s1", "singh");
					}
					System.out.println("reading : " + line);
					queries.add(line);
				}
			}
			System.out.println("end..." + queries.size());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			bufferedReader = null;
			fileReader.close();
		}
		return queries;
	}
}
