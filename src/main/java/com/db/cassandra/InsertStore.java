package com.db.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.PreparedStatement;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;

public class InsertStore implements Runnable {

	// Application settings
	private static String server_ip = "127.0.0.1";
	private static String keyspace = "mybook";

	// Application connection objects
	private static Cluster cluster = null;
	private static Session session = null;

	private Statement statement;
	private Thread thread;

	public static void openConnection() {
		if (cluster != null)
			return;

		cluster = Cluster.builder().addContactPoints(server_ip).build();

		final Metadata metadata = cluster.getMetadata();
		String msg = String.format("Connected to cluster: %s", metadata.getClusterName());
		System.out.println(msg);

		System.out.println("List of hosts");
		for (final Host host : metadata.getAllHosts()) {
			msg = String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(),
					host.getRack());
			System.out.println(msg);
		}
		session = cluster.connect();
		session.execute("use mybook");
	}

	public void connectMysql() {
		thread = new Thread(this);
		openConnection();
		String hostName = "localhost";
		String dbName = "mybook";
		String userName = "root";
		String password = "";
		Connection conn;
		try {
			conn = DriverManager.getConnection(
					"jdbc:mysql://" + hostName + ":3306/" + dbName + "?useUnicode=true&characterEncoding=UTF-8",
					userName, password);
			statement = conn.createStatement();

			thread.start();
		} catch (SQLException e) {
			// // TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void executeQuery(String query) {
		for (Row row : session.execute(query)) {
			System.out.println(row.toString());
		}
	}

	public static void insertQuery(String insert) {
		session.execute(insert);
		// com.datastax.driver.core.ResultSet result = session.execute(insert);
		// List<Row> arrRow = result.all();
		// for (Row row : arrRow) {
		// System.out.println(row.toString());
		// }
		// System.out.println("xong2");
	}

	public static void closeConnection() {
		if (cluster != null) {
			cluster.close();
			cluster = null;
			session = null;
		}
	}

	// Method definition
	public static void executeStatement(String statement) {
		openConnection();

		// session.execute(statement);
	}

	public static void main(String[] args) {
		InsertStore insertAuthor = new InsertStore();
		insertAuthor.connectMysql();
	}

	public void run() {
		ResultSet rs;
		String insertValue = "";
		// int id = 0;
		String name = "";
		String location = "";
		String country = "";
		String phone = "";
		for (int i = 1; i <= 500000; i++) {
			// id++;
			try {
				rs = statement.executeQuery("select * from store where store_id=" + i);
				while (rs.next()) {
					// id = rs.getInt("store_id");
					String nameTemp = rs.getString("store_name");
					name = nameTemp.replace("'", "''");
					location = rs.getString("location");
					country = rs.getString("country");
					phone = rs.getString("phonenumber");

				}
				insertValue = "INSERT INTO store(store_name,store_id,location,country,phonenumber,last_update) VALUES(\'"
						+ name + "\',now(),\'" + location + "\',\'" + country + "\',\'" + phone + "\',dateof(now()));";
				insertQuery(insertValue);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// }
			// insertValue += "APPLY BATCH;";
			if (i % 1000 == 0)
				System.out.println(i);
		}

	}

	public static String removeAccent(String s) {
		String temp = Normalizer.normalize(s, Normalizer.Form.NFD);
		Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
		return pattern.matcher(temp).replaceAll("");
	}

}
