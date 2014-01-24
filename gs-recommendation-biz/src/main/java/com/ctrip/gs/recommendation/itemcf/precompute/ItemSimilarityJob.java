package com.ctrip.gs.recommendation.itemcf.precompute;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityEntityWritable;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItem;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.gs.recommendation.AbstractJob;
import com.ctrip.gs.recommendation.hdfs.HdfsUtil;
import com.ctrip.gs.recommendation.hdfs.HdfsSequenceFileWriter;
import com.ctrip.gs.recommendation.util.TopSimilarItemsQueue;
import com.google.common.base.Preconditions;

/**
 * @author: wgji
 * @date：2013年12月27日 上午10:11:36
 * @comment:
 */
public class ItemSimilarityJob extends AbstractJob {
	private static final Logger logger = LoggerFactory.getLogger(ItemSimilarityJob.class);

	public static final String ITEM_ID_INDEX_PATH_STR = ItemSimilarityJob.class.getName() + ".itemIDIndexPathStr";
	public static final String MAX_SIMILARITIES_PER_ITEM = ItemSimilarityJob.class.getName() + ".maxSimilarItemsPerItem";

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ItemSimilarityJob(), args);
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Options options = initOpions();
		GenericOptionsParser option = new GenericOptionsParser(options, args);
		CommandLine commandLine = option.getCommandLine();
		if (commandLine.hasOption("help")) {
			new HelpFormatter().printHelp("item similarity", options);
			return -1;
		}

		int maxSimilarItemsPerItem = Integer.parseInt(getOptionValue(commandLine, "maxSimilarItemsPerItem",
				String.valueOf(DEFAULT_MAX_SIMILAR_ITEMS_PER_ITEM)));
		int maxPrefs = Integer.parseInt(getOptionValue(commandLine, "maxPrefs", String.valueOf(DEFAULT_MAX_PREFS)));
		int minPrefsPerUser = Integer.parseInt(getOptionValue(commandLine, "minPrefsPerUser",
				String.valueOf(DEFAULT_MIN_PREFS_PER_USER)));
		boolean booleanData = Boolean.valueOf(getOptionValue(commandLine, "booleanData", Boolean.FALSE.toString()));

		ToolRunner.run(getConf(), new PreparePreferenceMatrixJob(),
				new String[] { "--minPrefsPerUser", String.valueOf(minPrefsPerUser), "--booleanData",
						String.valueOf(booleanData), "--ratingShift", "0" });
		ToolRunner.run(getConf(),new RowSimilarityJob(),
				new String[] { "--maxObservationsPerRow", String.valueOf(maxPrefs), "--maxObservationsPerColumn",
						String.valueOf(maxPrefs), "--maxSimilaritiesPerRow", String.valueOf(maxSimilarItemsPerItem),
						"--excludeSelfSimilarity", Boolean.TRUE.toString() });

		HdfsUtil.rmr(getConf(), getItemcfTempPath(ITEM_SIMILARITY).toString());

		Job mostSimilarItemJob = new Job(getConf(), "mostSimilarItemJob");
		mostSimilarItemJob.setJarByClass(MostSimilarItemPairsMapper.class);
		mostSimilarItemJob.setMapperClass(MostSimilarItemPairsMapper.class);
		mostSimilarItemJob.setMapOutputKeyClass(EntityEntityWritable.class);
		mostSimilarItemJob.setMapOutputValueClass(DoubleWritable.class);
		mostSimilarItemJob.setReducerClass(MostSimilarItemPairsReducer.class);
		mostSimilarItemJob.setOutputKeyClass(EntityEntityWritable.class);
		mostSimilarItemJob.setOutputValueClass(DoubleWritable.class);
		mostSimilarItemJob.setInputFormatClass(SequenceFileInputFormat.class);
		mostSimilarItemJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(mostSimilarItemJob, getItemcfTempPath(SIMILARITY_MATRIX));
		FileOutputFormat.setOutputPath(mostSimilarItemJob, getItemcfTempPath(ITEM_SIMILARITY));
		Configuration mostSimilarItemsConf = mostSimilarItemJob.getConfiguration();
		mostSimilarItemsConf.set(ITEM_ID_INDEX_PATH_STR, getItemcfTempPath(ITEMID_INDEX).toString());
		mostSimilarItemsConf.setInt(MAX_SIMILARITIES_PER_ITEM, maxSimilarItemsPerItem);
		boolean succeeded = mostSimilarItemJob.waitForCompletion(true);
		if (!succeeded) {
			return -1;
		}
		logger.info("most similiar item job has been finished");
		HdfsSequenceFileWriter reader = new HdfsSequenceFileWriter(getConf(), getItemcfTempPath(ITEM_SIMILARITY).toString() + "/part-r-00000",
				new EntityEntityWritable(), new DoubleWritable());
		reader.read();

		return 0;
	}

	public static class MostSimilarItemPairsMapper extends
			Mapper<IntWritable, VectorWritable, EntityEntityWritable, DoubleWritable> {
		private OpenIntLongHashMap indexItemIDMap;
		private int maxSimilarItemsPerItem;

		@Override
		protected void setup(Context ctx) {
			Configuration conf = ctx.getConfiguration();
			maxSimilarItemsPerItem = conf.getInt(MAX_SIMILARITIES_PER_ITEM, -1);
			indexItemIDMap = TasteHadoopUtils.readIDIndexMap(conf.get(ITEM_ID_INDEX_PATH_STR), conf);

			Preconditions.checkArgument(maxSimilarItemsPerItem > 0, "maxSimilarItemsPerItem must be greater then 0!");
		}

		@Override
		protected void map(IntWritable itemIDIndexWritable, VectorWritable similarityVector, Context ctx) throws IOException,
				InterruptedException {
			int itemIDIndex = itemIDIndexWritable.get();

			TopSimilarItemsQueue topKMostSimilarItems = new TopSimilarItemsQueue(maxSimilarItemsPerItem);
			for (Vector.Element element : similarityVector.get().nonZeroes()) {
				SimilarItem top = topKMostSimilarItems.top();
				double candidateSimilarity = element.get();
				if (candidateSimilarity > top.getSimilarity()) {
					top.set(indexItemIDMap.get(element.index()), candidateSimilarity);
					topKMostSimilarItems.updateTop();
				}
			}

			long itemID = indexItemIDMap.get(itemIDIndex);
			for (SimilarItem similarItem : topKMostSimilarItems.getTopItems()) {
				long otherItemID = similarItem.getItemID();
				if (itemID < otherItemID) {
					ctx.write(new EntityEntityWritable(itemID, otherItemID), new DoubleWritable(similarItem.getSimilarity()));
				} else {
					ctx.write(new EntityEntityWritable(otherItemID, itemID), new DoubleWritable(similarItem.getSimilarity()));
				}
			}
		}
	}

	public static class MostSimilarItemPairsReducer extends
			Reducer<EntityEntityWritable, DoubleWritable, EntityEntityWritable, DoubleWritable> {
		@Override
		protected void reduce(EntityEntityWritable pair, Iterable<DoubleWritable> values, Context ctx) throws IOException,
				InterruptedException {
			ctx.write(pair, values.iterator().next());
		}
	}

	@Override
	protected Options initOpions() {
		Option maxSimilaritiesPerItem = buildOption("maxSimilaritiesPerItem",
				"try to cap the number of similar items per item to this number (default: " + DEFAULT_MAX_SIMILAR_ITEMS_PER_ITEM
						+ ')', String.valueOf(DEFAULT_MAX_SIMILAR_ITEMS_PER_ITEM));
		Option maxPrefs = buildOption("maxPrefs",
				"max number of preferences to consider per user or item,users or items with more preferences will be sampled down (default: "
						+ DEFAULT_MAX_PREFS + ')', String.valueOf(DEFAULT_MAX_PREFS));
		Option minPrefsPerUser = buildOption("minPrefsPerUser", "ignore users with less preferences than this (default: "
				+ DEFAULT_MIN_PREFS_PER_USER + ')', String.valueOf(DEFAULT_MIN_PREFS_PER_USER));
		Option booleanData = buildOption("booleanData", "Treat input as without pref values", Boolean.FALSE.toString());
		Option help = buildOption("help", "manual help document", "");

		Options options = new Options();
		options.addOption(maxSimilaritiesPerItem);
		options.addOption(maxPrefs);
		options.addOption(minPrefsPerUser);
		options.addOption(booleanData);
		options.addOption(help);

		return options;
	}
}