package com.db.cassandra;

import java.sql.ResultSet;
import java.text.Normalizer;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.javafaker.Faker;

public class InsertInventory implements Runnable {
	// Application settings
	private static String server_ip = "127.0.0.1";
	private static String keyspace = "mybook";

	// Application connection objects
	private static Cluster cluster = null;
	private static Session session = null;

	private Faker fakervi = new Faker(new Locale("en"));

	public InsertInventory() {
		openConnection();
	}

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

	public static Row executeQuery(String query) {
		String result = "";
		for (Row row : session.execute(query)) {
			return row;
		}
		return null;
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
		Thread[] thread = new Thread[1];

		for (int i = 0; i < thread.length; i++) {
			System.out.println(i + "Thread");
			thread[i] = new Thread(new InsertInventory());
			thread[i].start();
		}

	}

	public void run() {
		Random random = new Random();
		for (int count = 1; count <= 100000; count++) {

			Row rowStore = null;
			while (rowStore == null) {
				long randomStore = random.nextLong();
				rowStore = executeQuery("SELECT * FROM store WHERE token(store_id)>" + randomStore + " LIMIT 1");
			}
			String store = rowStore.getString("store_name").replaceAll("'","''");
			UUID storeId = rowStore.getUUID("store_id");

			Row rowBook = null;
			while (rowBook == null) {
				long randomBook = random.nextLong();
				rowBook = executeQuery("SELECT * FROM books WHERE token(book_id)>" + randomBook + " LIMIT 1");
			}
			String book = rowBook.getString("book_name").replaceAll("'","''");
			UUID bookId = rowBook.getUUID("book_id");
			String bookDescription = rowBook.getString("description").replaceAll("'","''");

			String insertBookByStore = "INSERT INTO book_by_store(inventory_id,store_id,store_name,book_id,book_name,book_description,last_update) VALUES "
					+ "(now()," + storeId + ",\'" + store + "\'," + bookId + ",\'" + book + "\',\'" + bookDescription
					+ "\',dateof(now()));";
			
//			System.out.println(insertBookByStore);

			String insertStoreByBook = "INSERT INTO store_by_book(inventory_id,store_id,store_name,book_id,book_name,book_description,last_update) VALUES "
					+ "(now()," + storeId + ",\'" + store + "\'," + bookId + ",\'" + book + "\',\'" + bookDescription
					+ "\',dateof(now()));";

			String insertValue = "BEGIN BATCH " + insertBookByStore + " " + insertStoreByBook + " APPLY BATCH;";
			insertQuery(insertValue);
			System.out.println(insertValue);
			//
			if (count % 1000 == 0)
				System.out.println(count);
		}

	}

}
