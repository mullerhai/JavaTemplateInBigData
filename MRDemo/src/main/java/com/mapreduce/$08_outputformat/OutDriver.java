package com.mapreduce.$08_outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());

        //设置最终输出的key的类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //设置OutputFormat
        job.setOutputFormatClass(MyOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\log.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\output"));

        job.waitForCompletion(true);
    }
}
