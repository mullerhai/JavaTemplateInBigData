package com.mapreduce.$03_flowbean;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FlowBean implements Writable {
    private int upFlow;
    private int downFlow;
    private int sumFlow;

    public FlowBean(){
    }

    public FlowBean(int upFlow, int downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    /*
        序列化时调用的方法
     */
    public void write(DataOutput out) throws IOException {
        //顺序无所谓
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }
    /*
        反序列化时调用的方法
     */
    public void readFields(DataInput in) throws IOException {
        //注意：反序列化时的顺序必须和序列化时的顺序保持一致
        upFlow = in.readInt();
        downFlow = in.readInt();
        sumFlow = in.readInt();
    }

    /*
        必须重写toString方法。否则最终输出的内容为对象的地址值。
     */
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }
}
