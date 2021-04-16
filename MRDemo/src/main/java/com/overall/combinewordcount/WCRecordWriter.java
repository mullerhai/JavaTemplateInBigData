package com.overall.combinewordcount;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class WCRecordWriter extends RecordWriter<Text, IntWritable> {
    FSDataOutputStream fos = null;

    public WCRecordWriter(TaskAttemptContext job) {

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            fos = fs.create(new Path(FileOutputFormat.getOutputPath(job) + "/word.txt"));
        } catch (Exception e) {
            e.printStackTrace();
            IOUtils.closeStream(fos);

            throw new RuntimeException();
        }
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        String word = key.toString();
        String times = value.toString();
        fos.write(word.getBytes(StandardCharsets.UTF_8));
        fos.write(times.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fos);
    }
}
