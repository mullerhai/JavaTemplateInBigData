package com.mapreduce.$02_WC_combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    KEYIN ： mapper输出的key
    VALUEIN : mapper输出的value
    KEYOUT : mapper输出的key
    VALUEOUT : mapper输出的value
 */

public class WCCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {

    private final IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //1.遍历所有的value
        for (IntWritable value : values) {
            sum += value.get();
        }
        //2.封装K,V
        outValue.set(sum);
        //3.写出K,V
        context.write(key, outValue);

    }
}
