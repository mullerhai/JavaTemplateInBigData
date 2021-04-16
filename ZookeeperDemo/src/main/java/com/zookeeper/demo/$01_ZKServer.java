package com.zookeeper.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

//服务器 ： 启动后在zk中创建临时节点
public class $01_ZKServer {
    public static void main(String[] args) {
        ZooKeeper zk = null;
        try {
            //1.创建连接对象
            String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
            int sessionTimeout = 4000;
            zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

                public void process(WatchedEvent event) {
                    //在该方法中去实现zk服务器返回通知后的操作
                }
            });

            //2.操作 - 在zk中创建临时节点
            //2.1判断父节点是否存在
            Stat exists = zk.exists("/demo", false);
            if (exists == null) {
                //2.1.1创建父节点
                zk.create("/node", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            //2.2创建临时节点
            zk.create("/demo/demoChildren3", "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            //3.让程序处于阻塞状态（持续运行）
            Thread.sleep(Long.MAX_VALUE);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //4.关资源
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
