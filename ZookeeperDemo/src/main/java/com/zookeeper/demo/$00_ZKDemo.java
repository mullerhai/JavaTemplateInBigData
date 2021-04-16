package com.zookeeper.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 通过API操作Zookeeper
 * 步骤：
 *  1.获取连接对象
 *  2.具体操作
 *  3.关资源
 */
public class $00_ZKDemo {

    private ZooKeeper zkClient;
    String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    int sessionTimeout = 4000;

    // 创建连接对象
    @Before
    public void connect() throws IOException {

        /*
            ZooKeeper(String connectString, int sessionTimeout, Watcher watcher)
            watcher : 监听器对象（在zk服务器需要通知客户端时就会调用该对象中的process方法）
         */
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            public void process(WatchedEvent event) {

                // 收到事件通知后的回调函数（用户的业务逻辑）
                //System.out.println(event.getType() + "--" + event.getPath());
            }
        });
    }

    // 关闭资源
    @After
    public void close() {
        if (zkClient != null) {
            try {
                zkClient.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // 创建子节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        /*
        create(final String path, byte data[], List<ACL> acl,CreateMode createMode)
            path : 创建节点的路径
            data : 该节点存放的数据
            acl ：访问控制权限
            createMode ：节点类型（永久不带序列号，永久带序列号，临时不带序列号，临时带序列号）
         */
        String path = zkClient.create("/demo/demoChildren3", "message".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println("创建的子节点路径为：" + path);
    }

    //删除节点
    @Test
    public void delete() throws KeeperException, InterruptedException {

        zkClient.delete("/demo/demoChildren1", -1);

    }

    // 判断子节点是否存在
    @Test
    public void exists() throws KeeperException, InterruptedException {
        /*
        exists(String path, boolean watch)
            path : 节点路径
            watch ： 是否使用默认的监听器对象（new Zookeeper上的监听器对象）
            返回值：如果为null表示该节点不存在
         */
        Stat exists = zkClient.exists("/demo", false);

        System.out.println(exists == null ? "不存在" : "存在");
    }

    //修改节点存储的数据
    @Test
    public void setData() throws KeeperException, InterruptedException {

        zkClient.setData("/demo", "message".getBytes(), -1);

    }

    // 持续监听子节点
    @Test
    public void listen() throws Exception {

        String path = "/demo";

        register(path);
        //阻塞程序，持续运行
        Thread.sleep(Long.MAX_VALUE);
    }

    public void register(final String path) throws Exception {
        /*
        getChildren(final String path, Watcher watcher)
            path : 父节点的路径
            watcher ：监听器对象（在zk服务器需要通知客户端时就会调用该对象中的process方法）
         */
        List<String> children = zkClient.getChildren(path, new Watcher() {

            //回调方法：在节点发生变化后 需要处理的业务逻辑
            public void process(WatchedEvent event) {
                //1.处理节点发生变化后的业务逻辑
                System.out.println(path + "节点发生了变化！");
                //2.再次注册监听
                try {
                    register(path);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        //输出节点存储的数据及子节点
        byte[] data = zkClient.getData(path, false, new Stat());
        System.out.println(path + " 节点存储的数据为：" + new String(data));
        System.out.println(path + " 节点的子节点如下：");
        for (String child : children) {
            System.out.println(child);
        }
    }

}