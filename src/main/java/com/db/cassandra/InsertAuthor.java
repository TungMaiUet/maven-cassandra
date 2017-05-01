package com.db.cassandra;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.regex.Pattern;

import javax.sound.midi.Soundbank;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class InsertAuthor implements Runnable {
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
		InsertAuthor insertAuthor = new InsertAuthor();
		insertAuthor.connectMysql();
	}

	public void run() {
		ResultSet rs;
		int id = 0;
		for (int count = 1; count <= 10; count++) {
			String insertValue = "";
			String name = "";
			String country = "";
			String email = "";
			for (int i = 1; i <= 10000; i++) {
				id++;
				try {
					rs = statement.executeQuery("select * from author where author_id=" + id);
					while (rs.next()) {
						id = rs.getInt("author_id");

						name = rs.getString("name");
						country = rs.getString("country");
						email = rs.getString("email");

						//
						//
					}
					insertValue = "INSERT INTO author(author_name,author_id,country,email,last_update) VALUES(\'" + name
							+ "\',now(),\'" + country + "\',\'" + email + "\',dateof(now()));";
//					System.out.println(insertValue);
					 insertQuery(insertValue);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println(count);
		}

	}

	public static String removeAccent(String s) {
		String temp = Normalizer.normalize(s, Normalizer.Form.NFD);
		Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
		return pattern.matcher(temp).replaceAll("");
	}

}
