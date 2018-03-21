/**
 * Copyright (C), 2011-2017, 微贷网.
 */
package com.weidai.mario.goods.biz.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(RedisGlobalLocker.class);

    private static final ThreadLocal<UuidLock> keyHolder = new ThreadLocal<>();

    private static final Random rnd = new Random();

    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * 获取锁
     * 
     * @param key
     * @param expx ex：秒，px：毫秒
     * @param time
     */
    public void lock(String key, final String expx, final long time) {
        UuidLock lock = new UuidLock(key);
        keyHolder.set(lock);
        long timeout = 200L;
        while (!tryLock(key, lock.getUuid(), expx, time)) {
            try {
                // 设置线程等待时间窗，取一个公平的随机数，防止出现线程饥饿
                Thread.sleep(randomLongWithBoundary(timeout));
            } catch (InterruptedException ignore) {
            }
        }
    }

    public boolean tryLock(String key, final String expx, final long time) {
        UuidLock lock = new UuidLock(key);
        keyHolder.set(lock);
        boolean permit = tryLock(key, lock.getUuid(), expx, time);
        if (!permit)
            keyHolder.remove();
        return permit;
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
        UuidLock lock = keyHolder.get();
        if (lock != null) {
            String key = lock.getKey();
            String uuid = lock.getUuid();
            List<Object> txResult = redisTemplate.execute(new SessionCallback<List<Object>>() {
                @Override
                public <K, V> List<Object> execute(RedisOperations<K, V> operations) throws DataAccessException {
                    StringRedisTemplate template = (StringRedisTemplate) operations;
                    template.watch(key);
                    if (uuid.equals(template.opsForValue().get(key))) {
                        template.multi();
                        template.delete(key);
                    }
                    return template.exec();
                }
            });
            if (txResult == null) {
                logger.warn("redis global lock: {} has been changed", key);
            }
        }
        keyHolder.remove();
    }

    private long randomLongWithBoundary(long max) {
        long min = 2L;
        return min + (long) (rnd.nextFloat() * (max - min));
    }

    private class UuidLock {
        private String key;

        private String uuid;

        UuidLock(String key) {
            this.key = key;
            this.uuid = UUID.randomUUID().toString();
        }

        String getKey() {
            return key;
        }

        String getUuid() {
            return uuid;
        }
    }
}
