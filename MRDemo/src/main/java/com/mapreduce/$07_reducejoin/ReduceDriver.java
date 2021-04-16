package com.mapreduce.$07_reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Job job = Job.getInstance(new Configuration());
        job.setMapperClass(ReduceMapper.class);
        job.setReducerClass(ReduceReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义分组类
        job.setGroupingComparatorClass(ReduceGroup.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\input\\reducejoin"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\output"));
        job.waitForCompletion(true);

    }
}
