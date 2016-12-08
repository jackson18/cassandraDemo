package com.qijiabin.cassandraDemo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;

/**
 * ========================================================
 * 日 期：2016年12月8日 上午11:09:14
 * 版 本：1.0.0
 * 类说明：
 * TODO
 * ========================================================
 * 修订日期     修订人    描述
 */
public class ConnectionTest {

	public static void main(String[] args) {
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Metadata metadata = cluster.getMetadata();
		
		for (Host host : metadata.getAllHosts()) {
			System.out.println(host);
		}
		
		System.out.println("********************");
		
		for (KeyspaceMetadata keyspaceMetadata : metadata.getKeyspaces()) {
			System.out.println(keyspaceMetadata.getName());
		}
		
		cluster.close();
	}

}
