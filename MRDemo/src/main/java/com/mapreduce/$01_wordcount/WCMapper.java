package com.mapreduce.$01_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/*
    Map阶段执行MapTask
    本质是对key和value的重组，最开始是<偏移量(位置),每行数据>
    后来是<map输出的key,map输出的value(这里是每一行的value，之后Reducer会对这个value汇总成一个新的Value)>
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    四个泛型分两组：
        第一组 ：
            KEYIN : 读取数据时的偏移量，一般都是LongWritable
            VALUEIN ：读取的数据（一行一行的数据）类型
        第二组：
            KEYOUT：写出去的key(单词)类型，key一般都是Text(String)类型
            VALUEOUT ：写出去的value（单词的数量）类型，可以是包装类或对象类

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
            //4.将key和value写出去
            context.write(outKey, outValue);
        }
    }
}
