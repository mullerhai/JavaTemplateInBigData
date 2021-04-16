package com.mapreduce.$05_comparablepartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phoneNumber = text.toString();

        if (phoneNumber.startsWith("136")){
            return 0;
        }else  if (phoneNumber.startsWith("137")){
            return 1;
        }else  if (phoneNumber.startsWith("138")){
            return 2;
        }else  if (phoneNumber.startsWith("139")){
            return 3;
        }else{
            return 4;
        }
    }
}
