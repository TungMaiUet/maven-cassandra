package com.db.cassandra;

import java.sql.Statement;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class InsertCategory implements Runnable {
	private Thread thread;

	private String[] categorys = { "Sách bình luận văn học‎", "Sách chính trị‎"," Sách giáo khoa"," Sách khoa học máy tính‎"," Sách địa chất",
			"Sách sinh học"," Sách toán học‎"," Sách vũ trụ","Sách khoa học viễn tưởng‎","Sách thiếu nhi","Sách thiếu niên","Sách lịch sử‎ ",
			"Truyện"};

	private static String server_ip = "127.0.0.1";
	private static String keyspace = "mybook";

	private static Cluster cluster = null;
	private static Session session = null;


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

	public String insertValue(String name) {
		String insert = "INSERT INTO category(category_id,category_name,last_update) VALUES " + "(now(),'"
				+ name + "',dateof(now()))";
		return insert;
	}

	public static void main(String[] args) {
		InsertCategory insert = new InsertCategory();
		insert.openConnection();

		// System.out.println(connectDb.insertValueBook());
	}

	public void run() {
		for (int i = 0; i < categorys.length; i++) {
//			System.out.println(insertValue(categorys[i]));
			 session.execute(insertValue(categorys[i]));
		}
	}
}