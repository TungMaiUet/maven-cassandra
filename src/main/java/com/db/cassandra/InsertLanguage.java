package com.db.cassandra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class InsertLanguage implements Runnable {
	private Thread thread;
	private Statement statement2;

	private String[] languages = { "Englist", "Vietnamese", "Korean", "German", "French" };
	
	private static String server_ip = "127.0.0.1";
	private static String keyspace = "mybook";
	
	private static Cluster cluster = null;
	private static Session session = null;

	private Statement statement;

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


	public String insertValue(String language_name) {
		String insert = "INSERT INTO language(language_id,language_name,last_update) VALUES " 
						+ "(now(),'"+language_name+"',dateof(now()))";
		return insert;
	}


	public static void main(String[] args) {
		InsertLanguage insertLanguage=new InsertLanguage();
		insertLanguage.openConnection();

		// System.out.println(connectDb.insertValueBook());
	}

	public void run() {
		for(int i=0;i<languages.length;i++){
			System.out.println(insertValue(languages[i]));
//			session.execute(insertValue(languages[i]));
		}
	}
}
