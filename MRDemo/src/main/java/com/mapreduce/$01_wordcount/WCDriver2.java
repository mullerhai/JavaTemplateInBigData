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
    在集群上跑Job

    命令 ： hadoop jar xxx.jar 全类名  参数1  参数2

           全类名 ：main方法所在的类的全类名
           参数1，参数2 ： 给args传的参数。

 */
public class WCDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //创建Job对象
        Job job = Job.getInstance(conf);
        //设置Jar加载路径---本地模式不设置也没问题
        job.setJarByClass(WCDriver2.class);

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
