package com.ctrip.gs.recommendation.dal.account;

import java.util.List;
import java.util.Map;

/**
 * @author: wgji
 * @date：2014年1月16日 下午1:59:56
 * @comment:
 */
@SuppressWarnings("rawtypes")
public interface UserBindMapper {
	public List<Map> getUserIds(List<String> bindUserNames);
}
