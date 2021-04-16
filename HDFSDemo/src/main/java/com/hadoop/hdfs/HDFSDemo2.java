package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/*
    通过配置文件创建文件系统对象

    注意：用户名的配置：
         选中当前类的类名(如果找不到先运行一次)
         --> EditConfigurations -->VM Options --> -DHADOOP_USER_NAME=YOUR_USERNAME

 */
public class HDFSDemo2 {
    @Test
    public void test() throws IOException {

        Configuration conf = new Configuration();
        //通过配置文件的方式设置HDFS的地址
        conf.set("fs.defaultFS","hdfs://hadoop102:9820");
        FileSystem fs = FileSystem.get(conf);


        fs.copyFromLocalFile(false, true,
                new Path("E:\\input\\log.txt"),new Path("/HDFSDemo/log1.txt"));


        fs.close();

    }
}
