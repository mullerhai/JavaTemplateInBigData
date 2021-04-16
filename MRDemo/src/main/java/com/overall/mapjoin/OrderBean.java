package com.overall.mapjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements Writable {
    private int id;
    private int sid;
    private String pname;
    private int count;

    public OrderBean() {
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(id);
        out.write(sid);
        out.writeBytes(pname);
        out.write(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        sid = in.readInt();
        pname = in.readUTF();
        count = in.readInt();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getSid() {
        return sid;
    }

    public void setSid(int sid) {
        this.sid = sid;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
