package com.ctrip.gs.recommendation.recommender;

import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import com.ctrip.gs.recommendation.eval.RecommenderEval;

/**
 * @author wgji
 * @date：2013年12月25日 下午3:11:50
 */
public class Recommender {
	public static void main(String[] args){
		
	}
	
	private final static int RECOMMENDER_NUM = 3;

	public static void itemCF(DataModel dataModel) throws TasteException {
		ItemSimilarity itemSimilarity = new UncenteredCosineSimilarity(dataModel);
		RecommenderBuilder recommenderBuilder = RecommenderFactory.itemRecommender(itemSimilarity, true);

		RecommenderEval.evaluate(RecommenderEval.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
				dataModel, 0.7);
		RecommenderEval.statsEvaluator(recommenderBuilder, null, dataModel, 2);

		LongPrimitiveIterator iter = dataModel.getUserIDs();
		while (iter.hasNext()) {
			long uid = iter.nextLong();
			List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM);
			RecommenderFactory.showItems(uid, list, true);
		}
	}

	public static void svd(DataModel dataModel) throws TasteException {
		RecommenderBuilder recommenderBuilder = RecommenderFactory.svdRecommender(new ALSWRFactorizer(dataModel, 10,
				0.05, 10));

		RecommenderEval.evaluate(RecommenderEval.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null,
				dataModel, 0.7);
		RecommenderEval.statsEvaluator(recommenderBuilder, null, dataModel, 2);

		LongPrimitiveIterator iter = dataModel.getUserIDs();
		while (iter.hasNext()) {
			long uid = iter.nextLong();
			List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM);
			RecommenderFactory.showItems(uid, list, true);
		}
	}
}
