package com.ctrip.gs.recommendation.dal.recomm;

import com.ctrip.gs.recommendation.bean.RecommendItem;

/** 
 * @author:  wgji
 * @date：2014年1月16日 下午1:59:56 
 * @comment: 
 */
public interface RecommendItemMapper {
	public RecommendItem getRecommendItem(int id);

	public int insertRecommendItem(RecommendItem item);

	public void updateRecommendItem(RecommendItem item);

	public void deleteRecommendItem(int id);
}
