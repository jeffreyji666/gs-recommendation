package com.ctrip.gs.recommendation.util;

/**
 * @author: wgji
 * @date：2013年12月30日 上午10:31:19
 * @comment:
 */
public class Constant {
	public static final int ONE_DAY_SECS = 3600 * 24;

	public static String HADOOP_HDSF_PATH = "hadoop.hdfs.path";
	public static String HADOOP_HDSF_USER_PATH = "hadoop.hdfs.user.path";
	public static String HADOOP_HDSF_ITEM_PATH = "hadoop.hdfs.item.path";
	public static String HADOOP_HDSF_INPUT_PATH = "hadoop.hdfs.input.path";
	public static String HADOOP_HDSF_ITEMCF_OUTPUT_PATH = "hadoop.hdfs.itemcf.output.path";
	public static String HADOOP_HDSF_ITEMCF_TMP_PATH = "hadoop.hdfs.itemcf.tmp.path";
	public static String HADOOP_HDSF_SVD_OUTPUT_PATH = "hadoop.hdfs.svd.output.path";
	public static String HADOOP_HDSF_SVD_TMP_PATH = "hadoop.hdfs.svd.tmp.path";

	public static final String HBASE_TABLE_POOL_MAX_SIZE = "hbase.table.pool.max.size";
	public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_ZOOKEEPER_ZNODE = "zookeeper.znode.parent";
	public static final String HBASE_TABLE_THREADS_MAX = "hbase.htable.threads.max";

	public static final int TABLE_POOL_MAX_SIZE = 50;
	public static final int DEFAULT_TABLE_THREADS_MAX = 500000;
	public static final int DEFAULT_TABLE_WRITER_BUFFER_SIZE = 1024 * 1024 * 6;
}