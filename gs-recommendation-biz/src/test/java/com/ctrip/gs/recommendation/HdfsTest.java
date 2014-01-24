package com.ctrip.gs.recommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ctrip.gs.recommendation.hdfs.HdfsSequenceFileWriter;
import com.ctrip.gs.recommendation.hdfs.HdfsUtil;
import com.ctrip.gs.recommendation.util.Config;
import com.ctrip.gs.recommendation.util.Constant;

/**
 * @author wgji
 * @date Dec 19, 2013 11:21:05 AM
 */
public class HdfsTest {
	private static Configuration conf;

	@BeforeClass
	public static void setUp() throws Exception {
		conf = new Configuration(true);
		conf.addResource("hdfs-site.xml");
		conf.set(Constant.HADOOP_HDSF_PATH, Config.getString(Constant.HADOOP_HDSF_PATH));
	}

	@Test
	public void testHdfs() {
		String hdfsPath = Config.getString("hadoop.hdfs.path") + "/test";
		HdfsSequenceFileWriter writer = new HdfsSequenceFileWriter(conf, hdfsPath, new IntWritable(), new IntWritable());
		try {
			for (int i = 0; i < 1001; i++)
				writer.write(new IntWritable(0), new IntWritable(i + 100));

			HdfsUtil.ls(conf, hdfsPath);
			writer.read();
			HdfsUtil.rmr(conf, hdfsPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}