package com.overall.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private final HashMap<String, String> hashMap = new HashMap<>();
    FileSystem fs = null;
    BufferedReader br = null;
    FSDataInputStream fis = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            fs = FileSystem.get(context.getConfiguration());
            URI[] cashFiles = context.getCacheFiles();
            fis = fs.open(new Path(cashFiles[0]));

            br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split("\t");
                hashMap.put(split[0], split[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            fs.close();
            br.close();
            fis.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] info = value.toString().split("\t");
        OrderBean outKey = new OrderBean();
        outKey.setId(Integer.parseInt(info[0]));
        outKey.setSid(Integer.parseInt(info[1]));
        outKey.setCount(Integer.parseInt(info[2]));
        outKey.setPname(hashMap.get(info[1]));
        context.write(outKey,NullWritable.get());
    }
}
