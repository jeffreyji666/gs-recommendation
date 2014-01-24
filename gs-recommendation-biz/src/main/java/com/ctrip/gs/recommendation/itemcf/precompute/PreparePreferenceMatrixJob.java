package com.ctrip.gs.recommendation.itemcf.precompute;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.ToEntityPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.ToItemPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.AbstractJob;
import com.ctrip.gs.recommendation.hdfs.HdfsSequenceFileWriter;
import com.ctrip.gs.recommendation.hdfs.HdfsUtil;
import com.ctrip.gs.recommendation.itemcf.mapreduce.ToItemVectorsMapper;
import com.ctrip.gs.recommendation.itemcf.mapreduce.ToItemVectorsReducer;
import com.ctrip.gs.recommendation.itemcf.mapreduce.ToUserVectorsReducer;
import com.ctrip.gs.recommendation.util.Config;
import com.ctrip.gs.recommendation.util.Constant;

/**
 * @author: wgji
 * @date：2013年12月27日 上午10:13:57
 * @comment:生成基础数据
 */
public class PreparePreferenceMatrixJob extends AbstractJob {
	private static final Logger logger = LoggerFactory.getLogger(PreparePreferenceMatrixJob.class);

	private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PreparePreferenceMatrixJob(), args);
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Options options = initOpions();
		GenericOptionsParser option = new GenericOptionsParser(options, args);
		CommandLine commandLine = option.getCommandLine();
		if (hasOption(commandLine, "help")) {
			new HelpFormatter().printHelp("prepare preference matrix", options);
			return -1;
		}
		int minPrefsPerUser = Integer.parseInt(getOptionValue(commandLine, "minPrefsPerUser",
				String.valueOf(DEFAULT_MIN_PREFS_PER_USER)));
		boolean booleanData = Boolean.valueOf(getOptionValue(commandLine, "booleanData", Boolean.FALSE.toString()));
		float ratingShift = Float.parseFloat(getOptionValue(commandLine, "ratingShift", "0"));

		// 删除已经使用过的空间
		Configuration conf = getConf();
		HdfsSequenceFileWriter reader = null;
		HdfsUtil.rmr(conf, getItemcfTempPath(ITEMID_INDEX).toString());
		HdfsUtil.rmr(conf, getItemcfTempPath(USER_VECTORS).toString());
		HdfsUtil.rmr(conf, getItemcfOutputPath(NUM_USERS).toString());
		HdfsUtil.rmr(conf, getItemcfTempPath(RATING_MATRIX).toString());
		HdfsUtil.ls(conf, Config.getString(Constant.HADOOP_HDSF_PATH));

		HdfsUtil.rmr(conf, Config.getString(Constant.HADOOP_HDSF_INPUT_PATH)+"/");
		HdfsUtil.copyFile(conf, "/opt/workspace/gs-recommendation/gs-recommendation-biz/src/main/java/com/ctrip/gs/recommendation/itemcf/precompute/item.csv", Config.getString(Constant.HADOOP_HDSF_INPUT_PATH)+"/item");
		HdfsUtil.ls(conf, Config.getString(Constant.HADOOP_HDSF_INPUT_PATH));

		Job itemIDIndexJob = new Job(getConf(), "itemIDIndexJob");
		itemIDIndexJob.setJarByClass(ItemIDIndexMapper.class);
		itemIDIndexJob.setMapperClass(ItemIDIndexMapper.class);
		itemIDIndexJob.setMapOutputKeyClass(VarIntWritable.class);
		itemIDIndexJob.setMapOutputValueClass(VarLongWritable.class);
		itemIDIndexJob.setReducerClass(ItemIDIndexReducer.class);
		itemIDIndexJob.setOutputKeyClass(VarIntWritable.class);
		itemIDIndexJob.setOutputValueClass(VarLongWritable.class);
		itemIDIndexJob.setCombinerClass(ItemIDIndexReducer.class);
		itemIDIndexJob.setInputFormatClass(TextInputFormat.class);
		itemIDIndexJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(itemIDIndexJob, getInputPath());
		FileOutputFormat.setOutputPath(itemIDIndexJob, getItemcfTempPath(ITEMID_INDEX));
		boolean succeeded = itemIDIndexJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("item id index job has been finished");
		reader = new HdfsSequenceFileWriter(conf, itemcfTempPath.toString() + "/" + ITEMID_INDEX + "/part-r-00000",
				new VarIntWritable(), new VarLongWritable());
		reader.read();

		Job toUserVectorJob = new Job(getConf(), "toUserVectorJob");
		toUserVectorJob.setJarByClass(ToItemPrefsMapper.class);
		toUserVectorJob.setMapperClass(ToItemPrefsMapper.class);
		toUserVectorJob.setMapOutputKeyClass(VarLongWritable.class);
		toUserVectorJob.setMapOutputValueClass(booleanData ? VarLongWritable.class : EntityPrefWritable.class);
		toUserVectorJob.setReducerClass(ToUserVectorsReducer.class);
		toUserVectorJob.setOutputKeyClass(VarLongWritable.class);
		toUserVectorJob.setOutputValueClass(VectorWritable.class);
		toUserVectorJob.setInputFormatClass(TextInputFormat.class);
		toUserVectorJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(toUserVectorJob, getInputPath());
		FileOutputFormat.setOutputPath(toUserVectorJob, getItemcfTempPath(USER_VECTORS));
		toUserVectorJob.getConfiguration().setInt(ToUserVectorsReducer.MIN_PREFERENCES_PER_USER, minPrefsPerUser);
		toUserVectorJob.getConfiguration().set(ToEntityPrefsMapper.RATING_SHIFT, String.valueOf(ratingShift));
		succeeded = toUserVectorJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("to user vector job has been finished");
		reader = new HdfsSequenceFileWriter(conf, itemcfTempPath.toString() + "/" + USER_VECTORS + "/part-r-00000",
				new VarLongWritable(), new VectorWritable());
		reader.read();

		// we need the number of users later
		int numberOfUsers = (int) toUserVectorJob.getCounters().findCounter(ToUserVectorsReducer.Counters.USERS).getValue();
		HdfsUtil.writeInt(getItemcfOutputPath(NUM_USERS).toString(), getConf(), numberOfUsers);
		logger.info("numberOfUsers:" + HdfsUtil.readInt(getItemcfOutputPath(NUM_USERS).toString(), getConf()));

		// build the rating matrix
		Job toItemVectorJob = new Job(getConf(), "toItemVectorJob");
		toItemVectorJob.setJarByClass(ToItemVectorsMapper.class);
		toItemVectorJob.setMapperClass(ToItemVectorsMapper.class);
		toItemVectorJob.setMapOutputKeyClass(IntWritable.class);
		toItemVectorJob.setMapOutputValueClass(VectorWritable.class);
		toItemVectorJob.setReducerClass(ToItemVectorsReducer.class);
		toItemVectorJob.setOutputKeyClass(IntWritable.class);
		toItemVectorJob.setOutputValueClass(VectorWritable.class);
		toItemVectorJob.setCombinerClass(ToItemVectorsReducer.class);
		toItemVectorJob.setInputFormatClass(SequenceFileInputFormat.class);
		toItemVectorJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(toItemVectorJob, getItemcfTempPath(USER_VECTORS));
		FileOutputFormat.setOutputPath(toItemVectorJob, getItemcfTempPath(RATING_MATRIX));
		succeeded = toItemVectorJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("to item vector job has been finished");
		reader = new HdfsSequenceFileWriter(conf, itemcfTempPath.toString() + "/" + RATING_MATRIX + "/part-r-00000",
				new IntWritable(), new VectorWritable());
		reader.read();

		return 0;
	}

	@Override
	protected Options initOpions() {
		Option minPrefsPerUser = buildOption("minPrefsPerUser", "ignore users with less preferences than this " + "(default: "
				+ DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
		Option booleanData = buildOption("booleanData", "Treat input as without pref values", Boolean.FALSE.toString());
		Option ratingShift = buildOption("ratingShift", "shift ratings by this value", "0");
		Option help = buildOption("help", "manual help document", "");

		Options options = new Options();
		options.addOption(minPrefsPerUser);
		options.addOption(booleanData);
		options.addOption(ratingShift);
		options.addOption(help);

		return options;
	}
}