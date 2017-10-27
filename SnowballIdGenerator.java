/**
 * Copyright (C), 2011-2017, 微贷网.
 */
package com.weidai.ucenterx.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * 参考twitter snowflake算法，使用 时间 + 自增sequence，没有NTP回刷时间的问题，简单粗暴
 * 将64 bit的long型数字拆成3部分，1 bit符号位：0， 中间31 bit表示时间（精确到秒），后32 bit表示自增sequence
 * 31 bit的时间可以使用2^31 / (1000 * 3600 * 24 * 365) = 68年，32 bit的sequence可以产生 2^32 - 1 = 42亿多个id，理论上足够使用
 * 
 * @author wuqi 2017/10/10 0010.
 */
@Component
public class SnowballIdGenerator {

    private static final String SNOW_BALL_SEQ_KEY = "snow_ball_seq";

    /**
     * 开始时间毫秒值：2017-01-01 00:00:00
     */
    private static final long startTime = 1_483_200_000_000L;

    /**
     * 自增sequence占用的位数
     */
    private static final long sequenceBits = 32L;

    @Autowired
    private StringRedisTemplate redisTemplate;
    
    /**
     * 获取下一个id，同步方法
     * @return
     */
    public long nextId() {
        long now = getNowMillis();
        long sequence = getIdSequence();
        return (((now - startTime) / 1_000L) << sequenceBits) | sequence;
    }

    private long getIdSequence() {
        return redisTemplate.opsForValue().increment(SNOW_BALL_SEQ_KEY, 1L);
    }

    private long getNowMillis() {
        return System.currentTimeMillis();
    }

    public static void main(String[] args) {
        LocalDateTime localDateTime = LocalDateTime.of(2017, 1, 1, 0, 0, 0, 0);
        System.out.println(localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }
}
