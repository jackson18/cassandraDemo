package com.qijiabin.cassandraDemo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * ========================================================
 * 日 期：2016年12月8日 下午2:31:05
 * 版 本：1.0.0
 * 类说明：
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class PreparedStatementDemo {
	
	private static final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	private static final Session session = cluster.connect();
	
	private static void createDataBase() {
		// 创建键空间
		String createKeySpaceCQL = "create keyspace if not exists myspace4 with replication={'class':'SimpleStrategy','replication_factor':1}";
		session.execute(createKeySpaceCQL);
	}
	
	private static void createTable() {
		// 创建列族
		String createTableCQL = "create table if not exists myspace4.student(name varchar primary key, age int)";
		session.execute(createTableCQL);
	}
	
	private static void insert() {
		// 插入数据
		PreparedStatement ps = session.prepare("insert into myspace4.student(name, age) values(?,?)");
		session.execute(ps.bind("wangwu", 1));
	}
	
	private static void query() {
		// 查询
		String queryCQL = "select * from myspace4.student";
		ResultSet rs = session.execute(queryCQL);
		for (Row row : rs.all()) {
			System.out.println("name: " + row.getString("name"));
			System.out.println("age: " + row.getInt("age"));
		}
	}
	
	private static void update() {
		// 修改
		PreparedStatement ps = session.prepare("update myspace4.student set age=? where name=?");
		session.execute(ps.bind(2, "wangwu"));
	}
	
	private static void delete() {
		// 删除
		PreparedStatement ps = session.prepare("delete from myspace4.student where name=?");
		session.execute(ps.bind("wangwu"));
	}
	
	private static void close() {
		session.close();
		cluster.close();
	}
	
	public static void main(String[] args) {
		createDataBase();
		createTable();
		insert();
		query();
		
		update();
		query();
		
		delete();
		query();
		
		close();
	}

}
