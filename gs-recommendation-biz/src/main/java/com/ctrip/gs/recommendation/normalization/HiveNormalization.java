package com.ctrip.gs.recommendation.normalization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ctrip.gs.recommendation.hdfs.HdfsUtil;
import com.ctrip.gs.recommendation.util.Config;
import com.ctrip.gs.recommendation.util.Constant;

/**
 * @author: wgji
 * @date：2014年1月15日 上午10:55:24
 * @comment:
 */
public class HiveNormalization {
	private static final Logger logger = LoggerFactory.getLogger(HiveNormalization.class);
	private static final AbstractApplicationContext context = new ClassPathXmlApplicationContext(
			"classpath*:applicationContext-biz.xml");
	private static final JdbcTemplate template = (JdbcTemplate) context.getBean("template");

	public static void main(String[] args) throws IOException {
		logger.info("begin to do hive normalization!");
		// load user_logging data and user bind data into hive
		loadHiveData();
		// generate user rating data by creating hive table as join query
		generateUserRatingData();
		logger.info("hive normalization has been finished!");
		System.exit(0);
	}

	private static void loadHiveData() throws IOException {
		template.execute("create database if not exists gs_recommendation location '/user/you/hive'");
		template.execute("use gs_recommendation");

		logger.info("begin to create user_logging table");
		StringBuilder userLoggingDDL = new StringBuilder();
		userLoggingDDL.append("create table if not exists user_logging(\n");
		userLoggingDDL.append("productid int,\n");
		userLoggingDDL.append("vid string comment 'user cookie string, 20 char length',\n");
		userLoggingDDL.append("start string comment 'the start time of user accessing the page',\n");
		userLoggingDDL.append("duration string comment 'the duration which user stay at the page',\n");
		userLoggingDDL.append("domain string comment 'domain of current page',\n");
		userLoggingDDL.append("userid string,\n");
		userLoggingDDL.append("signedin boolean,\n");
		userLoggingDDL.append("usergrade tinyint comment 'user ranking',\n");
		userLoggingDDL.append("startcitypkg smallint comment 'the start place of vocation product',\n");
		userLoggingDDL.append("orderid int \n");
		userLoggingDDL.append(") row format delimited fields terminated by ';'");
		template.execute(userLoggingDDL.toString());
		logger.info("user_logging table has been created");

		Configuration conf = getConf();
		HdfsUtil.ls(conf, "/user/you/hive");
		HdfsUtil.copyFile(conf, "/Users/you/Desktop/logging-data.csv", "/user/you/logging/logging");
		HdfsUtil.ls(conf, "/user/you/logging");

		logger.info("begin to load logging data into user_logging table");
		template.execute("load data inpath '/user/you/logging/logging' overwrite into table user_logging");
		logger.info("logging data has been loaded");

		logger.info("begin to create user_bind table");
		StringBuilder userBindDDL = new StringBuilder();
		userBindDDL.append("create table if not exists user_bind(\n");
		userBindDDL.append("userid int,\n");
		userBindDDL.append("bindusername string comment 'user name which related to user_logging userid',\n");
		userBindDDL.append("bindtypeid tinyint \n");
		userBindDDL.append(") row format delimited fields terminated by ','");
		template.execute(userBindDDL.toString());
		logger.info("user_bind table has been created");

		HdfsUtil.copyFile(conf, "/Users/you/Desktop/user-bind.csv", "/user/you/logging/bind");
		HdfsUtil.ls(conf, "/user/you/logging");

		logger.info("begin to load user bind data into user_bind table");
		template.execute("load data inpath '/user/you/logging/bind' overwrite into table user_bind");
		logger.info("user bind data has been loaded");
	}

	private static void generateUserRatingData() throws IOException {
		template.execute("drop table if exists user_rating");

		logger.info("begin to create user_rating table");
		StringBuilder userRatingDDL = new StringBuilder();
		userRatingDDL.append("create table user_rating row format delimited fields terminated by ',' \n");
		userRatingDDL.append("as \n");
		userRatingDDL.append("select bind.userid,logging.productid,count(*) \n");
		userRatingDDL
				.append("from (select * from user_logging where user_logging.signedin=true and user_logging.orderid=0)logging \n");
		userRatingDDL.append("join user_bind bind \n");
		userRatingDDL.append("on (logging.userid = bind.bindusername) \n");
		userRatingDDL.append("group by bind.userid,logging.productid \n");
		template.execute(userRatingDDL.toString());
		logger.info("user_rating table has been created");

		logger.info("begin to export user_rating table data into recommender job input directory");
		template.execute("insert overwrite directory '" + "/user/you/hive/user_rating' select * from user_rating");
		logger.info("user rating data has been exported");

		HdfsUtil.rmr(getConf(), Config.getString(Constant.HADOOP_HDSF_INPUT_PATH));
		HdfsUtil.ls(getConf(), "/user/you/hive");
	}

	private static Configuration getConf() {
		Configuration conf = new Configuration(true);
		conf.addResource("hdfs-site.xml");
		conf.set(Constant.HADOOP_HDSF_PATH, Config.getString(Constant.HADOOP_HDSF_PATH));

		return conf;
	}
}