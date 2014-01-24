package com.ctrip.gs.recommendation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.hive.service.ThriftHive.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: wgji
 * @date：2014年1月16日 下午3:14:12
 * @comment:test hive
 */
public class HiveTest {
	@Test
	public void testHiveThrift() throws HiveServerException, TException {
		TSocket transport = new TSocket("192.168.81.176", 10000);
		transport.setTimeout(1000);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		Client client = new ThriftHive.Client(protocol);
		transport.open();
		client.execute("show databases"); // Omitted HQL
		List<String> rows = null;
		while ((rows = client.fetchN(1000)) != null) {
			for (String row : rows) {
				System.out.println(row);
			}
		}
		transport.close();
	}

	@Test
	public void testHiveJdbc() throws SQLException, ClassNotFoundException {
		String hiveUrl = "jdbc:hive2://192.168.81.176:10000/default";
		String driverClassName = "org.apache.hive.jdbc.HiveDriver";
		Statement stmt = null;
		try {
			Class.forName(driverClassName);
			Connection conn = DriverManager.getConnection(hiveUrl);
			stmt = conn.createStatement();
			stmt.execute("create database if not exists gs_test location '/user/you/hive'");
			ResultSet rs = stmt.executeQuery("show databases");
			List<String> databases = new ArrayList<String>();
			while (rs.next())
				databases.add(rs.getString(1));
			Assert.assertTrue(databases.contains("gs_test"));

			stmt.execute("use gs_test");
			stmt.execute("create table if not exists gs_test.test(name string comment 'employee name', salary float comment 'employee salary')  comment 'description of the table'  location '/user/you/hive/test'");
			rs = stmt.executeQuery("show tables");
			List<String> tables = new ArrayList<String>();
			while (rs.next())
				tables.add(rs.getString(1));
			Assert.assertTrue(tables.contains("test"));

			stmt.execute("drop table test");
			rs = stmt.executeQuery("show tables");
			List<String> previousTables = new ArrayList<String>();
			while (rs.next())
				previousTables.add(rs.getString(1));
			Assert.assertFalse(previousTables.contains("test"));

			stmt.execute("drop database gs_test");
			rs = stmt.executeQuery("show databases");
			List<String> previousDatabases = new ArrayList<String>();
			while (rs.next())
				previousDatabases.add(rs.getString(1));
			Assert.assertFalse(previousDatabases.contains("gs_test"));
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}
}