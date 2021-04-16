package com.mapreduce.$08_outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
    自定义的OutputFormat

    FileOutputFormat<K, V>
        K: 如果前面是Reducer那就是Reducer的Key，如果前面是Mapper那就是Mapper的key，
           如果前面是InputFormat那就是InputFormat的key
        V: 如果前面是Reducer那就是Reducer的Value，如果前面是Mapper那就是Mapper的Value，
           如果前面是InputFormat那就是InputFormat的Value
 */
public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {
    /*
        接受一个job对象，返回以job对象为参数的RecordWriter对象
     */
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) {
        return new MyRecordWriter(job);
    }

}
