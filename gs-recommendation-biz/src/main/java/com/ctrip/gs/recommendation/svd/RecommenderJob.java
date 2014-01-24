package com.ctrip.gs.recommendation.svd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.als.MultithreadedSharingMapper;
import org.apache.mahout.cf.taste.hadoop.als.PredictionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.AbstractJob;

/**
 * <p>
 * Computes the top-N recommendations per user from a decomposition of the
 * rating matrix
 * </p>
 * 
 * <p>
 * Command line arguments specific to this class are:
 * </p>
 * 
 * <ol>
 * <li>--input (path): Directory containing the vectorized user ratings</li>
 * <li>--output (path): path where output should go</li>
 * <li>--numRecommendations (int): maximum number of recommendations per user
 * (default: 10)</li>
 * <li>--maxRating (double): maximum rating of an item</li>
 * <li>--numThreads (int): threads to use per mapper, (default: 1)</li>
 * </ol>
 */
public class RecommenderJob extends AbstractJob {
	private static final Logger logger = LoggerFactory.getLogger(RecommenderJob.class);

	static final String NUM_RECOMMENDATIONS = RecommenderJob.class.getName() + ".numRecommendations";
	static final String USER_FEATURES_PATH = RecommenderJob.class.getName() + ".userFeatures";
	static final String ITEM_FEATURES_PATH = RecommenderJob.class.getName() + ".itemFeatures";
	static final String MAX_RATING = RecommenderJob.class.getName() + ".maxRating";
	static final String USER_INDEX_PATH = RecommenderJob.class.getName() + ".userIndex";
	static final String ITEM_INDEX_PATH = RecommenderJob.class.getName() + ".itemIndex";

	static final int DEFAULT_NUM_RECOMMENDATIONS = 10;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RecommenderJob(), args);
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Options options = initOpions();
		GenericOptionsParser option = new GenericOptionsParser(options, args);
		CommandLine commandLine = option.getCommandLine();
		if (hasOption(commandLine, "help")) {
			new HelpFormatter().printHelp("svd recommend job", options);
			return -1;
		}

		Job predictionJob = new Job(getConf(), "predictionJob");
		predictionJob.setJarByClass(MultithreadedSharingMapper.class);
		predictionJob.setMapperClass(MultithreadedSharingMapper.class);
		predictionJob.setMapOutputKeyClass(IntWritable.class);
		predictionJob.setMapOutputValueClass(RecommendedItemsWritable.class);
		predictionJob.setInputFormatClass(SequenceFileInputFormat.class);
		predictionJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(predictionJob, getInputPath());
		FileOutputFormat.setOutputPath(predictionJob, getItemcfOutputPath());
		Configuration conf = predictionJob.getConfiguration();

		int numThreads = Integer.parseInt(getOptionValue(commandLine, "numThreads", String.valueOf(1)));
		conf.setInt(NUM_RECOMMENDATIONS,
				Integer.parseInt(getOptionValue(commandLine, "numRecommendations", String.valueOf(DEFAULT_NUM_RECOMMENDATIONS))));
		conf.set(USER_FEATURES_PATH, getOptionValue(commandLine, "userFeatures", Boolean.TRUE.toString()));
		conf.set(ITEM_FEATURES_PATH, getOptionValue(commandLine, "itemFeatures", Boolean.TRUE.toString()));
		conf.set(MAX_RATING, getOptionValue(commandLine, "maxRating", Boolean.TRUE.toString()));
		boolean usesLongIDs = Boolean.parseBoolean(getOptionValue(commandLine, "usesLongIDs", Boolean.TRUE.toString()));
		if (usesLongIDs) {
			conf.set(ParallelALSFactorizationJob.USES_LONG_IDS, String.valueOf(true));
			conf.set(USER_INDEX_PATH, getOptionValue(commandLine, "userIDIndex", Boolean.TRUE.toString()));
			conf.set(ITEM_INDEX_PATH, getOptionValue(commandLine, "itemIDIndex", Boolean.TRUE.toString()));
		}
		MultithreadedMapper.setMapperClass(predictionJob, PredictionMapper.class);
		MultithreadedMapper.setNumberOfThreads(predictionJob, numThreads);
		boolean succeeded = predictionJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("prediction job has been finished");
		return 0;
	}

	@Override
	protected Options initOpions() {
		Option userFeatures = buildOption("userFeatures", "path to the user feature matrix", Boolean.TRUE.toString());
		Option itemFeatures = buildOption("itemFeatures", "path to the item feature matrix", Boolean.TRUE.toString());
		Option numRecommendations = buildOption("numRecommendations", "number of recommendations per user",
				String.valueOf(DEFAULT_NUM_RECOMMENDATIONS));
		Option maxRating = buildOption("maxRating", "maximum rating available", Boolean.TRUE.toString());
		Option numThreads = buildOption("numThreads", "threads per mapper", String.valueOf(1));
		Option usesLongIDs = buildOption("usesLongIDs", "input contains long IDs that need to be translated",
				Boolean.TRUE.toString());
		Option userIDIndex = buildOption("userIDIndex", "index for user long IDs (necessary if usesLongIDs is true)",
				Boolean.TRUE.toString());
		Option itemIDIndex = buildOption("itemIDIndex", "index for user long IDs (necessary if usesLongIDs is true)",
				Boolean.TRUE.toString());
		Option help = buildOption("help", "manual help document", "");

		Options options = new Options();
		options.addOption(userFeatures);
		options.addOption(itemFeatures);
		options.addOption(numRecommendations);
		options.addOption(maxRating);
		options.addOption(numThreads);
		options.addOption(usesLongIDs);
		options.addOption(userIDIndex);
		options.addOption(itemIDIndex);
		options.addOption(help);

		return options;
	}
}