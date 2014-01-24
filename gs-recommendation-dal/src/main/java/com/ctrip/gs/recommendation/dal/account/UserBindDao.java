package com.ctrip.gs.recommendation.dal.account;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import com.google.common.base.Preconditions;

/**
 * @author: wgji
 * @date：2014年1月16日 下午2:02:57
 * @comment:
 */
@Repository("userBindDao")
@Scope("singleton")
public class UserBindDao {
	@Autowired
	private UserBindMapper userBindMapper;

	@SuppressWarnings("rawtypes")
	public Map<String, Long> getUserIds(List<String> bindUserNames) {
		Preconditions.checkArgument(bindUserNames != null && bindUserNames.size() > 0,
				"bindUserNames cannot be null when query user id");

		Map<String, Long> res = new HashMap<String, Long>();
		List<Map> nameIds = userBindMapper.getUserIds(bindUserNames);
		for (Map item : nameIds) {
			res.put((String) item.get("BindUserName"), (Long) item.get("UserID"));
		}

		return res;
	}
}