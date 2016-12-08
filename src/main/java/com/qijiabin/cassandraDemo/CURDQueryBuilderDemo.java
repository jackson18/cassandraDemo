package com.qijiabin.cassandraDemo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;

/**
 * ========================================================
 * 日 期：2016年12月8日 下午2:24:00
 * 版 本：1.0.0
 * 类说明：
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class CURDQueryBuilderDemo {

	private static final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	private static final Session session = cluster.connect();

	
	private static void insert() {
		// 插入数据
		Insert insert = QueryBuilder.insertInto("myspace2", "student").value("name", "lisi").value("age", 40);
		System.out.println(insert);
		session.execute(insert);
	}

	private static void query() {
		// 查询
		Where select = QueryBuilder.select().all().from("myspace2", "student").where(QueryBuilder.eq("name", "lisi"));
		System.out.println(select);
		ResultSet rs = session.execute(select);
		for (Row row : rs.all()) {
			System.out.println("name: " + row.getString("name"));
			System.out.println("age: " + row.getInt("age"));
		}
	}

	private static void update() {
		// 修改
		com.datastax.driver.core.querybuilder.Update.Where update = QueryBuilder.update("myspace2", "student")
				.with(QueryBuilder.set("age", 55)).where(QueryBuilder.eq("name", "lisi"));
		System.out.println(update);
		session.execute(update);
	}

	private static void delete() {
		// 删除
		com.datastax.driver.core.querybuilder.Delete.Where delete = QueryBuilder.delete().from("myspace2", "student")
				.where(QueryBuilder.eq("name", "lisi"));
		System.out.println(delete);
		session.execute(delete);
	}

	private static void close() {
		session.close();
		cluster.close();
	}

	public static void main(String[] args) {
		insert();
		query();

		update();
		query();

		delete();
		query();

		close();
	}

}
