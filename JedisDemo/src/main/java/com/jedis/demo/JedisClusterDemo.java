package com.jedis.demo;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JedisClusterDemo {

    public static void main(String[] args) {

        //线程池的配置
        JedisPoolConfig config = new JedisPoolConfig();
        //线程中连接数的最大数
        config.setMaxTotal(10);
        //线程中最多的空闲连接数
        config.setMaxIdle(5);
        //线程池中最小的空闲连接数
        config.setMinIdle(2);

        Set<HostAndPort> sets = new HashSet<HostAndPort>();
        sets.add(new HostAndPort("192.168.1.102", 6379));
        sets.add(new HostAndPort("192.168.1.102", 6380));
        JedisCluster cluster = new JedisCluster(sets, config);

        cluster.mset("{a}k1111", "v1", "{a}k2222", "v2", "{a}k3333", "v3");

        List<String> mget = cluster.mget("{a}k1111", "{a}k2111", "{a}k3333");

        System.out.println(mget);

    }
}
