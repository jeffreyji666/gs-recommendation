package com.ctrip.gs.recommendation.api;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.gs.recommendation.thrift.RecommendItemList;
import com.ctrip.gs.recommendation.thrift.RecommendParam;

public class RecommendHandler extends com.twitter.util.Function0<List<RecommendItemList>> {
	private RecommendParam param;

	public RecommendHandler(RecommendParam param) {
		this.param = param;
	}

	@Override
	public List<RecommendItemList> apply() {
		return new ArrayList<RecommendItemList>();
	}

}
