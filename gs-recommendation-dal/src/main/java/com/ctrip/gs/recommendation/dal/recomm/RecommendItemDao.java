package com.ctrip.gs.recommendation.dal.recomm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import com.ctrip.gs.recommendation.bean.RecommendItem;
import com.google.common.base.Preconditions;

/**
 * @author: wgji
 * @date：2014年1月16日 下午1:59:56
 * @comment:
 */
@Repository("recommendItemDao")
@Scope("singleton")
public class RecommendItemDao {
	@Autowired
	private RecommendItemMapper recommendItemMapper;

	public RecommendItem getRecommendItem(int id) {
		Preconditions.checkArgument(id > 0, "id must be positive when query item");

		return recommendItemMapper.getRecommendItem(id);
	}

	public int insertRecommendItem(RecommendItem item) {
		Preconditions.checkArgument(item != null, "item cannot be null when insert item to database");

		return recommendItemMapper.insertRecommendItem(item);
	}

	public void updateRecommendItem(RecommendItem item) {
		Preconditions.checkArgument(item != null, "item cannot be null when update item in database");

		recommendItemMapper.updateRecommendItem(item);
	}

	public void deleteRecommendItem(int id) {
		Preconditions.checkArgument(id > 0, "id must be positive when delete item");

		recommendItemMapper.deleteRecommendItem(id);
	}
}