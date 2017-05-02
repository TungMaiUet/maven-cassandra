package com.db.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;
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
import java.util.Locale;
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

	private Faker fakervi = new Faker(new Locale("vi"));

	public InsertStore() {
		// TODO Auto-generated constructor stub
		openConnection();
	}

	public void openConnection() {
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
		thread = new Thread(this);
		thread.start();
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

	public static void main(String[] args) {
		InsertStore insertStore = new InsertStore();
	}

	public void run() {
		String insertValue = "";
		for (int i = 1; i <= 500000; i++) {
			Name nameFaker = fakervi.name();
			String store = fakervi.book().publisher().replaceAll("'", "''");
			String location = "Việt Nam";
			String country = fakervi.address().cityName().replaceAll("'", "''");
			String phoneNumber = fakervi.phoneNumber().cellPhone();
			insertValue = "INSERT INTO store(store_name,store_id,location,country,phonenumber,last_update) VALUES(\'"
					+ store + "\',now(),\'" + location + "\',\'" + country + "\',\'" + phoneNumber
					+ "\',dateof(now()));";
			insertQuery(insertValue);
			if (i % 1000 == 0)
				System.out.println(i);
		}

	}
}
