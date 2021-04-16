package com.mapreduce.$01_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
    本地模式运行
    程序的入口：创建Job对象并配置
 */
public class WCDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //创建Job对象
        Job job = Job.getInstance(conf);
        //设置Jar加载路径---本地模式不设置也没问题
        job.setJarByClass(WCDriver.class);

        //可选优化：
        // 1. 设置InputFormatClass
        //job.setInputFormatClass(CombineTextInputFormat.class);
        //2. 设置虚拟切片最大值，结果信息可以看到 number of splits:1
        //CombineTextInputFormat.setMaxInputSplitSize(job,4194304);//4m

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
        FileInputFormat.setInputPaths(job,new Path("E:\\input\\wordcount"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\output"));
        //执行job
        //参数 ：是否打印进度
        //返回值 ：如果job执行成功返回true
        job.waitForCompletion(true);
    }
}
