package com.mapreduce.$07_reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private String fileName;

    /*
        在任务开始的时候只执行一次。在map方法执行前执行。
        作用 ：初始化（只执行一次的代码可以写在这个方法中）
     */
    @Override
    protected void setup(Context context) {
        //获取切片信息
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //获取文件的名字
        fileName = fileSplit.getPath().getName();
    }

    /*
        在任务结束的时候只执行一次。在map方法执行完后执行
        作用 ：关闭资源
     */
    @Override
    protected void cleanup(Context context) {
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.将Text转成String
        String line = value.toString();
        //2.切割数据
        String[] info = line.split("\t");
        //3.封装K,V
        OrderBean orderBean = null;
        if ("order.txt".equals(fileName)) {
            //注意：没有赋值的字段-pname一定不要赋值null因为该字段要排序
            orderBean = new OrderBean(info[0], info[1], "", info[2]);
        } else if ("pd.txt".equals(fileName)) {
            orderBean = new OrderBean("",info[0],info[1],"");
        }
        //4.写出k,v
        context.write(orderBean, NullWritable.get());
    }
}
