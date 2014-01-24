package com.ctrip.gs.recommendation.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wgji
 * @date：2013年12月25日 下午4:36:06
 */
public class HBaseClient {
	public static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);

	private static Configuration conf = null;
	static {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
	}

	private static HTablePool hTablePool;
	private static AtomicLong hbaseExceptionCount = new AtomicLong(0l);
	private static String ZK_NODE_PARENT = conf.get("hbase.zknode.parent") == null ? "/hbase" : conf.get("hbase.zknode.parent");
	private static final String zkPath = conf.get("hbase.zkquorum");

	public synchronized static HTablePool getHTablePool() {
		if (hTablePool == null) {
			try {
				logger.info("hbase.zkquorum = " + zkPath);
				hTablePool = HBaseClientManager.getClientManager().getHTablePool(zkPath, ZK_NODE_PARENT);
				if (hTablePool == null) {
					hTablePool = HBaseClientManager.getClientManager().addHTablePool(zkPath, ZK_NODE_PARENT);
				}
				logger.info("Hbase table pool was initialized successfully");
			} catch (Throwable e) {
				try {
					hTablePool.close();
				} catch (IOException e2) {
					logger.error("Failed to close hbase table pool", e2);
				}
				logger.error("Failed to initialize hbase table pool", e);
				throw new RuntimeException("Initialization failed", e);
			}
		}
		return hTablePool;
	}

	public static HTableInterface getHTable(byte[] name, boolean autoFlush) {
		HTableInterface hTableInterface = getHTablePool().getTable(name);
		hTableInterface.setAutoFlush(autoFlush);

		return hTableInterface;
	}

	public static HTableInterface getHTable(String name, boolean autoFlush) {
		return getHTable(name.getBytes(), autoFlush);
	}

	public static HTableInterface getHTable(String name) {
		return getHTable(name.getBytes(), false);
	}

	public static void closeHTable(HTableInterface table) {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				logger.error("Close hbase table error.", e);
			}
		}
	}

	public static AtomicLong getHbaseExceptionCount() {
		return hbaseExceptionCount;
	}

	public static void incrHbaseExceptionCount() {
		hbaseExceptionCount.incrementAndGet();
	}

	public static void shutdown() throws IOException {
		HBaseClientManager.getClientManager().shutdown();
	}

	public static void flushTable(HTableInterface table) throws IOException {
		if (table != null) {
			table.flushCommits();
		}
	}
}