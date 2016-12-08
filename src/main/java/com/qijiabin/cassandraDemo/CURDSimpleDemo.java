package com.qijiabin.cassandraDemo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * ========================================================
 * 日 期：2016年12月8日 上午11:32:41
 * 版 本：1.0.0
 * 类说明：
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class CURDSimpleDemo {
	
	private static final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	private static final Session session = cluster.connect();
	
	private static void createDataBase() {
		// 创建键空间
		String createKeySpaceCQL = "create keyspace if not exists myspace2 with replication={'class':'SimpleStrategy','replication_factor':1}";
		session.execute(createKeySpaceCQL);
	}
	
	private static void createTable() {
		// 创建列族
		String createTableCQL = "create table if not exists myspace2.student(name varchar primary key, age int)";
		session.execute(createTableCQL);
	}
	
	private static void insert() {
		// 插入数据
		String insertCQL = "insert into myspace2.student(name, age) values('zhangsan', 1)";
		session.execute(insertCQL);
	}
	
	private static void query() {
		// 查询
		String queryCQL = "select * from myspace2.student";
		ResultSet rs = session.execute(queryCQL);
		for (Row row : rs.all()) {
			System.out.println("name: " + row.getString("name"));
			System.out.println("age: " + row.getInt("age"));
		}
	}
	
	private static void update() {
		// 修改
		String updateCQL = "update myspace2.student set age=2 where name='zhangsan'";
		session.execute(updateCQL);
	}
	
	private static void delete() {
		// 删除
		String deleteCQL = "delete from myspace2.student where name='zhangsan'";
		session.execute(deleteCQL);
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

