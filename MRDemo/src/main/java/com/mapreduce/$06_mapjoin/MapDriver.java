package com.mapreduce.$06_mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MapDriver.class);
        job.setMapperClass(MapMapper.class);

        // 本例中无需reduceTask，设置为我0
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //添加缓存文件的路径
        job.addCacheFile(new URI("file:///E:/input/reducejoin/pd.txt"));

        //该文件不是要缓存的文件
        FileInputFormat.setInputPaths(job,new Path("E:\\input\\reducejoin\\order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\output111"));

        job.waitForCompletion(true);
    }
}
