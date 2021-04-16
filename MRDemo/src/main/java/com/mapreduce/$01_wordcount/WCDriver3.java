package com.mapreduce.$01_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/*
    在本地向集群提交Job:
    1.设置配置
        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:9820");
        //指定MR运行在Yarn上
        conf.set("mapreduce.framework.name","yarn");
        //指定MR可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname","hadoop103");
     2.打包
     3.①将job.setJarByClass(WCDriver3.class);注释掉
       ②添加job.setJar(jar包路径)
     4.在Edit Configurations中配置
        VM Options : -DHADOOP_USER_NAME=atguigu
        Program Arguments : hdfs://hadoop101:9820/input  hdfs://hadoop101:9820/output

 */
public class WCDriver3 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop101:9820");
        //指定MR运行在Yarn上
        conf.set("mapreduce.framework.name","yarn");
        //指定MR可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname", "hadoop102");

        //创建Job对象
        Job job = Job.getInstance(conf);

        //设置Jar加载路径
        //job.setJarByClass(WCDriver3.class);

        job.setJar("C:\\Users\\YOGA\\Documents\\MRDemo\\target\\MRDemo-1.0-SNAPSHOT.jar");

        //设置Mapper和Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        //设置Mapper输出的k,v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的k,v类型（在这是Reducer输出的k,v类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置数据源读出和输出的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行job
        //参数 ：是否打印进度
        //返回值 ：如果job执行成功返回true
        job.waitForCompletion(true);
    }
}
