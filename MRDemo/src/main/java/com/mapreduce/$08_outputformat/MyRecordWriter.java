package com.mapreduce.$08_outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream atguiguFOS;
    private FSDataOutputStream otherFOS;

    //创建流
    public MyRecordWriter(TaskAttemptContext job){
        try {
            //创建FileSystem的对象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //创建流，用的是文件系统的create方法，参数为输出文件的路径，要具体到文件名
            atguiguFOS = fs.create(new Path(FileOutputFormat.getOutputPath(job) + "/atguigu.txt"));
            otherFOS = fs.create(new Path(FileOutputFormat.getOutputPath(job) + "/other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
            //关流
            IOUtils.closeStream(atguiguFOS);
            IOUtils.closeStream(otherFOS);
            //程序退出
            throw new RuntimeException("流出问题了");
        }
    }
    /*
        写出数据
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        //将Text转成String
        String address = value.toString() + "\n";
        //判断是否包含了atguigu
        if (address.contains("atguigu")){
            //写到atguigu.txt
            atguiguFOS.write(address.getBytes());
        }else{
            //写到other.txt
            otherFOS.write(address.getBytes());
        }
    }

    /*
        用来关闭资源
     */
    @Override
    public void close(TaskAttemptContext context) {
        IOUtils.closeStream(atguiguFOS);
        IOUtils.closeStream(otherFOS);
    }
}
