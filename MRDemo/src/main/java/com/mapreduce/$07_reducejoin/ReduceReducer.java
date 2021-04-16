package com.mapreduce.$07_reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReduceReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    /*
                key                               value
     null     1     null    小米
     1001     1      1      null                   null
     1004     1      4      null

     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //1.先获取第一条key的数据（有pname的数据）
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();//获取第一条key
        //1.1获取名字
        String name = key.getPname();

        //2.用获取到的名字替换后面的所有的数据并写出
        //2.1遍历第二条key以后所有的数据
        while(iterator.hasNext()){
            iterator.next();
            key.setPname(name);
            //2.2写出数据
            context.write(key,NullWritable.get());
        }
    }
}
