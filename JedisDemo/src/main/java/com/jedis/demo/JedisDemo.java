package com.jedis.demo;

import redis.clients.jedis.Jedis;

import java.util.List;

public class JedisDemo {

    public static void main(String[] args) {

        //创建redis客户端
        Jedis jedis = new Jedis("hadoop102", 6379);

        System.out.println(jedis.ping());

        jedis.set("name","charlie");

        jedis.mset("k1","v1","k2","v2","k3","v3");

        List<String> mget = jedis.mget("k1", "k2", "k3");

        System.out.println(mget);

        jedis.close();
    }
}
