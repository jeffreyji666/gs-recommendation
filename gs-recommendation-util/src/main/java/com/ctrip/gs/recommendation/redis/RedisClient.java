package com.ctrip.gs.recommendation.redis;

import com.ctrip.gs.recommendation.util.Config;
import com.ctrip.gs.recommendation.util.JEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import com.google.common.base.Function;

public class RedisClient {
	public static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

	private static final String hosts = Config.getString("redis.host");
	private static final Integer MAX_ACTIVE = Config.getInt("redis.maxActive");

	private ShardedJedisPool pool;

	public RedisClient() {
		try {
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxActive(MAX_ACTIVE);
			poolConfig.setTestWhileIdle(true);
			poolConfig.setTimeBetweenEvictionRunsMillis(1000L * 120);
			poolConfig.setTestOnBorrow(true);

			ArrayList<JedisShardInfo> jedisShardInfos = new ArrayList<JedisShardInfo>();
			for (String host : hosts.split(",")) {
				String[] hostPort = host.split(":");
				JedisShardInfo jinfo = new JedisShardInfo(hostPort[0], Integer.parseInt(hostPort[1]));
				jedisShardInfos.add(jinfo);
			}

			pool = new ShardedJedisPool(poolConfig, jedisShardInfos);
		} catch (Exception e) {
			logger.error("Failed to init redis client.", e);
			throw new IllegalStateException("Failed to init redis client.", e);
		}
	}

	/**
	 * Set an object in redis cache
	 * 
	 * @param key
	 * @param expire
	 * @param val
	 */
	public void set(final String key, final int expire, final Object val) {
		try {
			JEntry<Object> e = new JEntry<Object>(val);
			final String json = e.toJson();
			if (json != null) {
				JedisUtil.using(pool).call(new Function<ShardedJedis, String>() {
					@Override
					public String apply(ShardedJedis jedis) {
						jedis.setex(key, expire, json);
						return null;
					}
				});
			}
		} catch (Exception e) {
			logger.error("Failed to set object to redis. [key=" + key + ", val=" + val + "]", e);
		}
	}

	/**
	 * Get an object from redis cache
	 * 
	 * @param key
	 * @param bs
	 * @return
	 */
	public Object getObject(final String key) {
		try {
			String json = JedisUtil.using(pool).call(new Function<ShardedJedis, String>() {
				@Override
				public String apply(ShardedJedis jedis) {
					return jedis.get(key);
				}
			});
			if (json != null) {
				return JEntry.parseJson(json, Object.class);
			} else {
				return null;
			}
		} catch (Exception e) {
			logger.error("Failed to get object from redis. [key=" + key + "]", e);
			return null;
		}
	}

	/**
	 * Add a string into a redis set
	 * 
	 * @param key
	 * @param members
	 * @return
	 */
	public long sadd(final String key, final String... members) {
		try {
			Long res = JedisUtil.using(pool).call(new Function<ShardedJedis, Long>() {
				@Override
				public Long apply(ShardedJedis jedis) {
					return jedis.sadd(key, members);
				}
			});
			return res == null ? 0L : res.longValue();
		} catch (Exception e) {
			logger.error("Failed to sadd to redis. [key=" + key + "]", e);
			return 0L;
		}
	}

	/**
	 * Remove a string from a redis set
	 * 
	 * @param key
	 * @param members
	 * @return
	 */
	public long srem(final String key, final String... members) {
		try {
			Long res = JedisUtil.using(pool).call(new Function<ShardedJedis, Long>() {
				@Override
				public Long apply(ShardedJedis jedis) {
					return jedis.srem(key, members);
				}
			});
			return res == null ? 0L : res.longValue();
		} catch (Exception e) {
			logger.error("Failed to srem from redis. [key=" + key + "]", e);
			return 0L;
		}
	}

	/**
	 * Return the number of members in a redis set
	 * 
	 * @param key
	 * @return
	 */
	public long scard(final String key) {
		try {
			Long res = JedisUtil.using(pool).call(new Function<ShardedJedis, Long>() {
				@Override
				public Long apply(ShardedJedis jedis) {
					return jedis.scard(key);
				}
			});
			return res == null ? 0L : res.longValue();
		} catch (Exception e) {
			logger.error("Failed to scard from redis. [key=" + key + "]", e);
			return 0L;
		}
	}

	/**
	 * Return all the members in a redis set
	 * 
	 * @param key
	 * @return
	 */
	public Set<String> smembers(final String key) {
		try {
			Set<String> res = JedisUtil.using(pool).call(new Function<ShardedJedis, Set<String>>() {
				@Override
				public Set<String> apply(ShardedJedis jedis) {
					return jedis.smembers(key);
				}
			});
			if (res == null) {
				res = new HashSet<String>();
			}
			return res;
		} catch (Exception e) {
			logger.error("Failed to smember from redis. [key=" + key + "]", e);
			return new HashSet<String>();
		}
	}

	/**
	 * Delete an key in redis
	 * 
	 * @param key
	 */
	public void delete(final String key) {
		try {
			JedisUtil.using(pool).call(new Function<ShardedJedis, String>() {
				@Override
				public String apply(ShardedJedis jedis) {
					jedis.del(key);
					return null;
				}
			});
		} catch (Exception e) {
			logger.error("Failed to delete from redis. [key=" + key + "]", e);
		}
	}

	public void shutdown() {
		pool.destroy();
	}
}
