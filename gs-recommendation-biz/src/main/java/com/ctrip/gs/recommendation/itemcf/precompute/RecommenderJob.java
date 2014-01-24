package com.ctrip.gs.recommendation.itemcf.precompute;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.item.PartialMultiplyMapper;
import org.apache.mahout.cf.taste.hadoop.item.PrefAndSimilarityColumnWritable;
import org.apache.mahout.cf.taste.hadoop.item.SimilarityMatrixRowWrapperMapper;
import org.apache.mahout.cf.taste.hadoop.item.ToVectorAndPrefReducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.AbstractJob;
import com.ctrip.gs.recommendation.hdfs.HdfsUtil;
import com.ctrip.gs.recommendation.hdfs.HdfsSequenceFileWriter;
import com.ctrip.gs.recommendation.itemcf.mapreduce.AggregateAndRecommendReducer;
import com.ctrip.gs.recommendation.itemcf.mapreduce.UserVectorSplitterMapper;
import com.ctrip.gs.recommendation.util.Config;
import com.ctrip.gs.recommendation.util.Constant;

/**
 * @author: wgji
 * @date：2014年1月6日 上午11:32:35
 * @comment: 推荐job
 */
public class RecommenderJob extends AbstractJob {
	private static final Logger logger = LoggerFactory.getLogger(RecommenderJob.class);

	public static final String BOOLEAN_DATA = "booleanData";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RecommenderJob(), args);
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Options options = initOpions();
		GenericOptionsParser option = new GenericOptionsParser(options, args);
		CommandLine commandLine = option.getCommandLine();
		if (hasOption(commandLine, "help")) {
			new HelpFormatter().printHelp("recommender job", options);
			return -1;
		}
		int maxPrefsPerUser = Integer.parseInt(getOptionValue(commandLine, "maxPrefsPerUser",
				String.valueOf(UserVectorSplitterMapper.DEFAULT_MAX_PREFS_PER_USER_CONSIDERED)));
		int numRecommendations = Integer.parseInt(getOptionValue(commandLine, "numRecommendations",
				String.valueOf(AggregateAndRecommendReducer.DEFAULT_NUM_RECOMMENDATIONS)));
		boolean booleanData = Boolean.valueOf(getOptionValue(commandLine, "booleanData", Boolean.FALSE.toString()));
		String userFile = Config.getString(Constant.HADOOP_HDSF_USER_PATH);
		String itemFile = Config.getString(Constant.HADOOP_HDSF_ITEM_PATH);
		Path partialMultiplyPath = getItemcfTempPath("partialMultiply");
		
		Configuration conf = getConf();
		HdfsSequenceFileWriter reader = null;
		HdfsUtil.rmr(conf, partialMultiplyPath.toString());
		HdfsUtil.rmr(conf, getItemcfOutputPath().toString());

		// run the item similarity job
		ToolRunner.run(getConf(), new ItemSimilarityJob(), args);

		// start the multiplication of the co-occurrence matrix by the user
		// vectors
		Job partialMultiplyJob = new Job(getConf(), "partialMultiplyJob");
		partialMultiplyJob.setJarByClass(ToVectorAndPrefReducer.class);
		partialMultiplyJob.setMapOutputKeyClass(VarIntWritable.class);
		partialMultiplyJob.setMapOutputValueClass(VectorOrPrefWritable.class);
		partialMultiplyJob.setReducerClass(ToVectorAndPrefReducer.class);
		partialMultiplyJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		partialMultiplyJob.setOutputKeyClass(VarIntWritable.class);
		partialMultiplyJob.setOutputValueClass(VectorAndPrefsWritable.class);
		MultipleInputs.addInputPath(partialMultiplyJob, getItemcfTempPath(SIMILARITY_MATRIX), SequenceFileInputFormat.class,
				SimilarityMatrixRowWrapperMapper.class);
		MultipleInputs.addInputPath(partialMultiplyJob, getItemcfTempPath(USER_VECTORS), SequenceFileInputFormat.class,
				UserVectorSplitterMapper.class);
		Configuration partialMultiplyConf = partialMultiplyJob.getConfiguration();
		partialMultiplyConf.setBoolean("mapred.compress.map.output", true);
		partialMultiplyConf.set("mapred.output.dir", partialMultiplyPath.toString());
		if (userFile != null) {
			partialMultiplyConf.set(UserVectorSplitterMapper.USERS_FILE, userFile);
		}
		partialMultiplyConf.setInt(UserVectorSplitterMapper.MAX_PREFS_PER_USER_CONSIDERED, maxPrefsPerUser);
		boolean succeeded = partialMultiplyJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("partial multiply job has been finished");
		reader = new HdfsSequenceFileWriter(conf, partialMultiplyPath.toString() + "/part-r-00000", new VarIntWritable(), new VectorAndPrefsWritable());
		reader.read();
		
		// extract out the recommendations
		Job aggregateAndRecommendJob = new Job(getConf(), "countObservationJob");
		aggregateAndRecommendJob.setJarByClass(PartialMultiplyMapper.class);
		aggregateAndRecommendJob.setMapperClass(PartialMultiplyMapper.class);
		aggregateAndRecommendJob.setMapOutputKeyClass(VarLongWritable.class);
		aggregateAndRecommendJob.setMapOutputValueClass(PrefAndSimilarityColumnWritable.class);
		aggregateAndRecommendJob.setReducerClass(AggregateAndRecommendReducer.class);
		aggregateAndRecommendJob.setOutputKeyClass(VarLongWritable.class);
		aggregateAndRecommendJob.setOutputValueClass(RecommendedItemsWritable.class);
		aggregateAndRecommendJob.setInputFormatClass(SequenceFileInputFormat.class);
		aggregateAndRecommendJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(aggregateAndRecommendJob, partialMultiplyPath);
		FileOutputFormat.setOutputPath(aggregateAndRecommendJob, getItemcfOutputPath("recommendation"));
		Configuration aggregateAndRecommendConf = aggregateAndRecommendJob.getConfiguration();
		if (itemFile != null) {
			aggregateAndRecommendConf.set(AggregateAndRecommendReducer.ITEMS_FILE, itemFile);
		}
		setIOSort(aggregateAndRecommendJob);
		aggregateAndRecommendConf.set(AggregateAndRecommendReducer.ITEMID_INDEX_PATH, getItemcfTempPath(ITEMID_INDEX)
				.toString());
		aggregateAndRecommendConf.setInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, numRecommendations);
		aggregateAndRecommendConf.setBoolean(BOOLEAN_DATA, booleanData);
		succeeded = aggregateAndRecommendJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("aggregate and recommend job has been finished");
		reader = new HdfsSequenceFileWriter(conf, getItemcfOutputPath("recommendation").toString() + "/part-r-00000", new VarLongWritable(), new RecommendedItemsWritable());
		reader.read();
		
		return 0;
	}

	private static void setIOSort(JobContext job) {
		Configuration conf = job.getConfiguration();
		conf.setInt("io.sort.factor", 100);
		// new arg name
		String javaOpts = conf.get("mapred.map.child.java.opts");
		if (javaOpts == null) {
			javaOpts = conf.get("mapred.child.java.opts"); // old arg name
		}
		int assumedHeapSize = 512;
		if (javaOpts != null) {
			Matcher m = Pattern.compile("-Xmx([0-9]+)([mMgG])").matcher(javaOpts);
			if (m.find()) {
				assumedHeapSize = Integer.parseInt(m.group(1));
				String megabyteOrGigabyte = m.group(2);
				if ("g".equalsIgnoreCase(megabyteOrGigabyte)) {
					assumedHeapSize *= 1024;
				}
			}
		}
		// Cap this at 1024MB now; see
		// https://issues.apache.org/jira/browse/MAPREDUCE-2308
		conf.setInt("io.sort.mb", Math.min(assumedHeapSize / 2, 1024));
		// For some reason the Merger doesn't report status for a long time;
		// increase timeout when running these jobs
		conf.setInt("mapred.task.timeout", 60 * 60 * 1000);
	}

	@Override
	protected Options initOpions() {
		Option numRecommendations = buildOption("numRecommendations", "Number of recommendations per user",
				String.valueOf(AggregateAndRecommendReducer.DEFAULT_NUM_RECOMMENDATIONS));
		Option booleanData = buildOption("booleanData", "Treat input as without pref values", Boolean.FALSE.toString());
		Option maxPrefsPerUser = buildOption("maxPrefsPerUser",
				"Maximum number of preferences considered per user in final recommendation phase",
				String.valueOf(UserVectorSplitterMapper.DEFAULT_MAX_PREFS_PER_USER_CONSIDERED));
		Option help = buildOption("help", "manual help document", "");

		Options options = new Options();
		options.addOption(numRecommendations);
		options.addOption(booleanData);
		options.addOption(maxPrefsPerUser);
		options.addOption(help);

		return options;
	}
}