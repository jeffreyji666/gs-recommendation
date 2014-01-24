package com.ctrip.gs.recommendation.eval;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.model.DataModel;

/**
 * @author wgji
 * @date：2013年12月25日 下午3:01:25
 */
public class RecommenderEval {
	/**
	 * evaluator
	 */
	public enum EVALUATOR {
		AVERAGE_ABSOLUTE_DIFFERENCE, RMS
	}

	public static RecommenderEvaluator buildEvaluator(EVALUATOR type) {
		switch (type) {
		case RMS:
			return new RMSRecommenderEvaluator();
		case AVERAGE_ABSOLUTE_DIFFERENCE:
		default:
			return new AverageAbsoluteDifferenceRecommenderEvaluator();
		}
	}

	public static void evaluate(EVALUATOR type, RecommenderBuilder rb, DataModelBuilder mb, DataModel dm, double trainPt)
			throws TasteException {
		System.out.printf("%s Evaluater Score:%s\n", type.toString(),
				buildEvaluator(type).evaluate(rb, mb, dm, trainPt, 1.0));
	}

	public static void evaluate(RecommenderEvaluator re, RecommenderBuilder rb, DataModelBuilder mb, DataModel dm,
			double trainPt) throws TasteException {
		System.out.printf("Evaluater Score:%s\n", re.evaluate(rb, mb, dm, trainPt, 1.0));
	}

	/**
	 * statsEvaluator
	 */
	public static void statsEvaluator(RecommenderBuilder rb, DataModelBuilder mb, DataModel m, int topn)
			throws TasteException {
		RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
		IRStatistics stats = evaluator.evaluate(rb, mb, m, null, topn,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		// System.out.printf("Recommender IR Evaluator: %s\n", stats);
		System.out.printf("Recommender IR Evaluator: [Precision:%s,Recall:%s]\n", stats.getPrecision(),
				stats.getRecall());
	}
}
