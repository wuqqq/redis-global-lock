/**
 * Copyright (C), 2011-2017, 微贷网.
 */
package com.weidai.ucenterx.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * redis分布式锁
 * 
 * @author wuqi 2017/10/10 0010.
 */
@Component
public class RedisGlobalLocker {

    private static final ThreadLocal<Map<String, String>> keyHolder = new ThreadLocal<>();

    private static final Random rnd = new Random();

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private SnowballIdGenerator snowballIdGenerator;

    /**
     * 获取锁
     * 
     * @param key
     * @param expx ex：秒，px：毫秒
     * @param time
     */
    public void lock(String key, final String expx, final long time) {
        String id = String.valueOf(snowballIdGenerator.nextId());
        Map<String, String> keyId = new HashMap<>(1);
        keyId.put(key, id);
        keyHolder.set(keyId);
        long timeout = 200L;
        while (!tryLock(key, id, expx, time)) {
            try {
                // 设置线程等待时间窗，取一个公平的随机数，防止出现线程饥饿
                Thread.sleep(randomLongWithBoundary(timeout));
            } catch (InterruptedException e) {
            }
        }
    }

    private long randomLongWithBoundary(long max) {
        long min = 1L;
        return min + (long) (rnd.nextFloat() * (max - min));
    }

    private boolean tryLock(String key, String value, final String expx, final long time) {
        String rs = redisTemplate.execute((RedisCallback<String>) connection -> {
            Jedis jedis = (Jedis) connection.getNativeConnection();
            return jedis.set(key, value, "NX", expx.toUpperCase(), time);
        });
        if ("OK".equals(rs)) {
            return true;
        }
        return false;
    }

    public void unlock() {
        Map<String, String> keyId = keyHolder.get();
        if (keyId != null) {
            Optional<Map.Entry<String, String>> optional = keyId.entrySet().stream().findFirst();
            if (optional.isPresent()) {
                final String key = optional.get().getKey();
                final String value = optional.get().getValue();
                redisTemplate.execute(new SessionCallback<Object>() {
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    @Override
                    public Object execute(RedisOperations operations) throws DataAccessException {
                        if (value.equals(redisTemplate.opsForValue().get(key))) {
                            operations.watch(key);
                            operations.multi();
                            redisTemplate.delete(key);
                            return operations.exec();
                        }
                        return null;
                    }
                });
            }
        }
		keyHolder.remove();
    }
}
