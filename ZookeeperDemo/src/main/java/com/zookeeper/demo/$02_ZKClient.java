package com.zookeeper.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/*
    客户端 ： 监听节点变化
 */
public class $02_ZKClient {
    private static ZooKeeper zk = null;
    public static void main(String[] args) {
        try {
            //1.创建连接对象
            String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
            int sessionTimeout = 4000;
            zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

                public void process(WatchedEvent event) {

                }
            });

            //2.监听节点
            //2.1判断父节点是否存在
            Stat exists = zk.exists("/demo", false);
            if (exists == null) {//表示父节点不存在
                //2.1.1创建父节点
                zk.create("/demo", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            //2.2获取子节点并监听--一直监听
            register();

            //3.让程序一直运行--因为客户端得一直监听
            Thread.sleep(Long.MAX_VALUE);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //4.关资源
            if (zk != null){
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    //2.2获取子节点并监听
    public static void register() throws Exception {
        List<String> children = zk.getChildren("/demo", new Watcher() {
            public void process(WatchedEvent event) {
                //1.处理节点改变后的业务逻辑
                System.out.println("节点发生了变化！");
                //2.再次监听
                try {
                    register();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        for (String child : children) {
            System.out.println(child);
        }
    }
}
