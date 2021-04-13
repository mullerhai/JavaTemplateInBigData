package com.jedis.demo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Set;

public class SentinelPoolDemo {

    /**
     * 连接主从模式+哨兵
     */
    public static void main(String[] args) {

        //获取哨兵连接池
        Set<String> set = new HashSet<String>();
        set.add("hadoop102:26379");

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(2);
        config.setMaxIdle(5);
        JedisSentinelPool poll = new JedisSentinelPool("mymaster", set, config);
        Jedis jedis = poll.getResource();

        System.out.println(jedis.ping());

        jedis.close();
    }
}
