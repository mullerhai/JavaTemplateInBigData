package com.mapreduce.$03_flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int sumUpFlow = 0;
        int sumDownFlow = 0;
        //1.遍历同一个手机号(key)的所有value
        for (FlowBean value: values ) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        //2.封装K,V
        FlowBean outValue = new FlowBean(sumUpFlow, sumDownFlow);
        //3.写出K,V
        context.write(key,outValue);
    }
}
