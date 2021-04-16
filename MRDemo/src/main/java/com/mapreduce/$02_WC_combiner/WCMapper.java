package com.mapreduce.$02_WC_combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    Map阶段MapTask需要做的事情

    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    四个泛型分两组：
        第一组 ：
            KEYIN : 读取数据时的偏移量
            VALUEIN ：读取的数据（一行一行的数据）
        第二组：
            KEYOUT：写出去的key(单词)
            VALUEOUT ：写出去的value（单词的数量）

 */
public class WCMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    //封装的key
    private final Text outKey = new Text();
    //封装的value
    private final IntWritable outValue = new IntWritable();
    /**
     * map方法中实现MapTask中需要实现的功能
     * 该方法会被循环调用每调用一次传入一行数据
     * @param key ：读取数据时的偏移量
     * @param value ：读取的数据（一行一行的数据）
     * @param context ：上下文（在这是用来写出K,V）
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.将value转换成String
        String line = value.toString();
        //2.切割数据
        String[] words = line.split(" ");
        //3.封装K,V
        for (String word : words) {
                outKey.set(word);
                outValue.set(1);
            }
            //4.将key和value写出去
            context.write(outKey, outValue);

        }
    }

