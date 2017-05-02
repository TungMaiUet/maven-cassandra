package com.db.cassandra;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.Locale;
import java.util.regex.Pattern;

import javax.sound.midi.Soundbank;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;

public class InsertAuthor implements Runnable {
	// Application settings
	private static String server_ip = "127.0.0.1";
	private static String keyspace = "mybook";

	// Application connection objects
	private static Cluster cluster = null;
	private static Session session = null;

	private Thread thread;
	
	private Faker fakervi = new Faker(new Locale("vi"));
	
	public InsertAuthor() {
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
		InsertAuthor insertAuthor = new InsertAuthor();
	}

	public void run() {
		int id = 0;
		for (int count = 1; count <= 100000; count++) {
			String insertValue = "";
			Name nameFaker = fakervi.name();
			String name = nameFaker.fullName().replaceAll("'", "''");
			String country = fakervi.address().cityName().replaceAll("'","''");
			String email =nameFaker.lastName()+nameFaker.firstName()+"@gmail.com";
			insertValue = "INSERT INTO author(author_name,author_id,country,email,last_update) VALUES(\'" + name
					+ "\',now(),\'" + country + "\',\'" + email + "\',dateof(now()));";
			// System.out.println(insertValue);
			insertQuery(insertValue);
			System.out.println(count);
		}

	}

}
