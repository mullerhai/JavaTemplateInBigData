package com.overall.combinewordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WCOutputFormat extends FileOutputFormat<Text, IntWritable> {

    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job){
        return new WCRecordWriter(job);
    }
}
