package com.overall.partedsortedflowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phoneNum = text.toString();

        if (phoneNum.startsWith("136")) {
            return 0;
        } else if (phoneNum.startsWith("137")) {
            return 1;
        } else {
            return 2;
        }
    }
}
