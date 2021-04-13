package com.jedis.demo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolDemo {

    /**
     * jedis线程池
     */
    public static void main(String[] args) {

        //线程池的配置
        JedisPoolConfig config = new JedisPoolConfig();
        //线程中连接数的最大数
        config.setMaxTotal(10);
        //线程中最多的空闲连接数
        config.setMaxIdle(5);
        //线程池中最小的空闲连接数
        config.setMinIdle(2);
        JedisPool jedisPool = new JedisPool(config,"hadoop102",6379);

        //从线程池中获取连接
        Jedis jedis = jedisPool.getResource();

        System.out.println(jedis.ping());

        //将连接还给线程池
        jedis.close();
    }
}
