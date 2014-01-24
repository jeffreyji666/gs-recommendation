package com.ctrip.gs.recommendation.store;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.ctrip.gs.recommendation.bean.RItem;
import com.ctrip.gs.recommendation.hbase.HBaseClient;

/**
 * @author: wgji
 * @date：2014年1月23日 上午10:14:12
 * @comment:
 */
public class HBaseRecommendation {
	public static final Logger logger = LoggerFactory.getLogger(HBaseRecommendation.class);

	public static String RECOMMEND_RESULT_TABLE = "recommend_result";
	public static byte[] RECOMMEND_COLUMN_FAMILY = "recommendations".getBytes();
	public static byte[] RECOMMEND_CONTENT_QUALIFIER = "recommend_item".getBytes();

	public static void main(String[] args) throws IOException {
		createHBaseSchema();
	}

	private static void createHBaseSchema() throws IOException {
		HBaseAdmin admin = null;
		try {
			Configuration config = HBaseConfiguration.create();
			config.addResource("hbase-site.xml");

			admin = new HBaseAdmin(config);
			String table = "recommend_item";
			if (admin.tableExists(table)) {// 如果存在要创建的表，那么先删除，再创建
				admin.disableTable(table);
				admin.deleteTable(table);
				logger.info(table + " does exists,detele it before creating it");
			}
			HTableDescriptor desc = new HTableDescriptor(table);
			desc.addFamily(new HColumnDescriptor("userid"));
			desc.addFamily(new HColumnDescriptor("recommend_item"));

			admin.createTable(desc);
		} catch (MasterNotRunningException e) {
			logger.error("master not running error happened in creating hbase recommendation schema:", e);
		} catch (ZooKeeperConnectionException e) {
			logger.error("zookeeper connection error happened in creating hbase recommendation schema:", e);
		} catch (IOException e) {
			logger.error("io error happened in creating hbase recommendation schema:", e);
		} finally {
			admin.close();
		}
	}

	public static void batchAddRecommendItem(int userId, long timestamp, List<RItem> items) {
		Put put = new Put(String.valueOf(userId).getBytes(), HConstants.LATEST_TIMESTAMP);
		JSONArray vs = (JSONArray) JSON.toJSON(items);
		put.add(RECOMMEND_COLUMN_FAMILY, RECOMMEND_CONTENT_QUALIFIER, timestamp, vs.toJSONString().getBytes());
		HTableInterface table = HBaseClient.getHTable(RECOMMEND_RESULT_TABLE);
		try {
			table.put(put);
		} catch (IOException e) {
			flushTableUntilSuccess(table);
		} finally {
			HBaseClient.closeHTable(table);
		}
	}

	public static void deleteRecommendItem(String rowkey) {
		HTableInterface table = HBaseClient.getHTable(RECOMMEND_RESULT_TABLE);
		try {
			Delete delete = new Delete(rowkey.getBytes());
			table.delete(delete);
		} catch (IOException e) {
			flushTableUntilSuccess(table);
		} finally {
			flushTableUntilSuccess(table);
		}

	}

	private static void flushTableUntilSuccess(HTableInterface table) {
		int i = 0;
		while (true) {
			try {
				Thread.sleep(1000);

				i++;
				table.flushCommits();

				break;
			} catch (IOException e) {
				logger.warn(Bytes.toString(table.getTableName()) + " Flush failed: " + i, e);
			} catch (InterruptedException e) {
				logger.warn(Bytes.toString(table.getTableName()) + " Flush failed: " + i, e);
			}
		}
	}
}