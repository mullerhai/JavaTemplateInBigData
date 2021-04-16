package com.mapreduce.$04_comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    第一组泛型 ： mapper输出的key和value的类型 （对象，手机号）
    第二组泛型 ： 将key,value类型调顺序。（手机号，对象）
 */
public class FlowReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    /*
            key                 value
        180 180 360         15322222222
        100 260 360         13422222222

        总结 ： 对遍历value时，key也会跟着变化。
                key会变成value所对应的key的数据。
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text phoneNumber : values) {
            //获取到一组手机号和对应的流量对象
            //写出K,V
            context.write(phoneNumber, key);
        }
    }
}
