package com.ctrip.gs.recommendation.recommender;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import com.ctrip.gs.recommendation.eval.RecommenderEval;

/**
 * @author wgji
 * @date：2013年12月25日 下午2:44:59
 */
public final class RecommenderFactory {

	/**
	 * build Data model from file
	 */
	public static DataModel buildDataModel(String file) throws TasteException, IOException {
		return new FileDataModel(new File(file));
	}

	public static DataModel buildDataModelNoPref(String file) throws TasteException, IOException {
		return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(new FileDataModel(new File(file))));
	}

	public static DataModelBuilder buildDataModelNoPrefBuilder() {
		return new DataModelBuilder() {
			@Override
			public DataModel buildDataModel(FastByIDMap<PreferenceArray> trainingData) {
				return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(trainingData));
			}
		};
	}

	public static RecommenderBuilder itemRecommender(final ItemSimilarity is, boolean pref) throws TasteException {
		return pref ? new RecommenderBuilder() {
			@Override
			public Recommender buildRecommender(DataModel model) throws TasteException {
				return new GenericItemBasedRecommender(model, is);
			}
		} : new RecommenderBuilder() {
			@Override
			public Recommender buildRecommender(DataModel model) throws TasteException {
				return new GenericBooleanPrefItemBasedRecommender(model, is);
			}
		};
	}

	public static RecommenderBuilder svdRecommender(final Factorizer factorizer) throws TasteException {
		return new RecommenderBuilder() {
			@Override
			public Recommender buildRecommender(DataModel dataModel) throws TasteException {
				return new SVDRecommender(dataModel, factorizer);
			}
		};
	}

	public static RecommenderBuilder svd(DataModel dataModel) throws TasteException {
		RecommenderBuilder recommenderBuilder = RecommenderFactory.svdRecommender(new ALSWRFactorizer(dataModel, 5,
				0.05, 10));

		RecommenderEval.evaluate(RecommenderEval.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
				dataModel, 0.7);
		RecommenderEval.statsEvaluator(recommenderBuilder, null, dataModel, 2);

		return recommenderBuilder;
	}

	public static void showItems(long uid, List<RecommendedItem> recommendations, boolean skip) {
		if (!skip || recommendations.size() > 0) {
			System.out.printf("uid:%s,", uid);
			for (RecommendedItem recommendation : recommendations) {
				System.out.printf("(%s,%f)", recommendation.getItemID(), recommendation.getValue());
			}
			System.out.println();
		}
	}
}