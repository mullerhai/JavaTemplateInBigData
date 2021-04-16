package com.overall.partedsortedflowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private final Text outValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] phoneInfo = value.toString().split("\t");
        FlowBean outKey = new FlowBean(Long.parseLong(phoneInfo[phoneInfo.length - 3]),
                Long.parseLong(phoneInfo[phoneInfo.length - 2]));
        outValue.set(phoneInfo[1]);
        context.write(outKey,outValue);
    }
}
