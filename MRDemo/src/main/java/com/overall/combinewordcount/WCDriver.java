package com.overall.combinewordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 以WordCount案例为基准，增添如下功能/优化
 *
 * 1. InputFormat使用CombineTextInputFormat合并小文件
 * 2. Mapper排序后增加自定义Combiner类进行局部合并
 * 3. OutputFormat使用自定义OutputFormat输出到word.txt
 *
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WCDriver.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setCombinerClass(WCCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(WCOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\input\\wordcount"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\output"));

        job.waitForCompletion(true);

    }
}
