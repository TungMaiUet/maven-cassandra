package com.db.cassandra;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.LongStream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.javafaker.Faker;

public class InsertBook implements Runnable {
	// Application settings
	private static String server_ip = "127.0.0.1";
	private static String keyspace = "mybook";

	// Application connection objects
	private static Cluster cluster = null;
	private static Session session = null;

	private Faker fakervi = new Faker(new Locale("en"));

	public InsertBook() {
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

	// // Method definition
	// public static void executeStatement(String statement) {
	// openConnection();
	//
	// // session.execute(statement);
	// }

	public static void main(String[] args) {
		Thread[] thread = new Thread[10];

		for (int i = 0; i < thread.length; i++) {
			System.out.println(i + "Thread");
			thread[i] = new Thread(new InsertBook());
			thread[i].start();
		}

	}

	public void run() {
		ResultSet rs;
		Random random = new Random();
		// int id = 1000000 * threadCount;
		for (int count = 1; count <= 10000000; count++) {
			String insertValueBook = "";
			String insertValueBookByAuthor = "";
			String insertValueBookByCategory = "";
			String insertValueBookByLanguage = "";
			String name = fakervi.book().title().replaceAll("'", "''");
			String description = fakervi.name().fullName().replaceAll("'", "''");
			String publicationDate = "2017-04-25";
			String language = "";
			UUID languageId = null;

			String category = "";
			UUID categoryId = null;
			int countAuthor = random.nextInt(4) + 1;
			String authors = "";
			for (int ath = 0; ath < countAuthor; ath++) {
				Row row = null;
				while (row == null) {
					long randomAuthor = random.nextLong();
					row = executeQuery("SELECT * FROM author WHERE token(author_id)>" + randomAuthor + " LIMIT 1");
				}
				String authorName = row.getString("author_name");
				UUID authorId = row.getUUID("author_id");
				String country = row.getString("country");
				String email = row.getString("email");
				String authorByBook = "INSERT INTO book_by_author(author_name,author_id,book_id,book_name,book_description,last_update) VALUES "
						+ "(\'" + authorName + "\'," + authorId + ",now(),\'" + name + "\',\'" + description
						+ "\',dateof(now()));";
				String authorBook = "{author_name:\'" + authorName + "\',author_id:" + authorId + ",country:\'"
						+ country + "\',email:\'" + email + "\'}";
				authors += authorBook + ",";
				insertValueBookByAuthor += authorByBook + " ";
			}
			authors = authors.substring(0, authors.length() - 1);

			// language insert
			Row rowLanguage = null;
			while (rowLanguage == null) {
				long randomLanguage = random.nextLong();
				rowLanguage = executeQuery(
						"SELECT * FROM language WHERE token(language_id)>" + randomLanguage + " LIMIT 1");
			}
			language = rowLanguage.getString("language_name");
			languageId = rowLanguage.getUUID("language_id");
			insertValueBookByLanguage = "INSERT INTO book_by_language(language_id,language_name,book_id,book_name,book_description,last_update) VALUES "
					+ "(" + languageId + ",\'" + language + "\',now(),\'" + name + "\',\'" + description
					+ "\',dateof(now()));";

			// category insert
			Row rowCategory = null;
			while (rowCategory == null) {
				long randomCategory = random.nextLong();
				rowCategory = executeQuery(
						"SELECT * FROM category WHERE token(category_id)>" + randomCategory + " LIMIT 1");
			}
			category = rowCategory.getString("category_name");
			categoryId = rowCategory.getUUID("category_id");

			insertValueBookByCategory = "INSERT INTO book_by_category(category_id,category_name,book_id,book_name,book_description,last_update) VALUES "
					+ "(" + categoryId + ",\'" + category + "\',now(),\'" + name + "\',\'" + description
					+ "\',dateof(now()));";

			insertValueBook = "INSERT INTO books(book_name,book_id,description,publication_date,language,categorys,authors,last_update) VALUES(\'"
					+ name + "\',now(),\'" + description + "\',\'" + publicationDate + "\',\'" + language + "\',[\'"
					+ category + "\'],[" + authors + "],dateof(now()));";

			// summ string insert
			String insertValue = "BEGIN BATCH " + insertValueBookByAuthor + " " + insertValueBook + " "
					+ insertValueBookByCategory + " " + insertValueBookByLanguage + " APPLY BATCH;";
			// System.out.println(insertValue);
			insertQuery(insertValue);
			//
			if (count % 5000 == 0)
				System.out.println(count);
		}

	}

}
