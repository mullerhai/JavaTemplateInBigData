package com.mapreduce.$06_mapjoin;

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
import java.util.Map;

/**
 * MapJoin,在Mapper阶段完成数据的合并，类似于SQL的join
 * 用于处理有多个输入表时，主表与附表有相同的数据列，将两表的数据合并输出的情况
 * 用FileInputFormat读取主表数据，附表数据缓存在内存中，在setup阶段读入缓存
 * 写出主表数据同时写出附表数据
 */
public class MapMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    //用来存放需要缓存的数据
    //key : pid
    //Value : pname
    private final Map<String,String> map = new HashMap<>();
    /*
        将pd.txt缓存到内存中
     */
    @Override
    protected void setup(Context context) throws IOException {
        FileSystem fs = null;
        BufferedReader br = null;
        FSDataInputStream fis = null;
        try {
            //1.创建流
            fs = FileSystem.get(context.getConfiguration());
            //1.1获取缓存路径
            URI[] cacheFiles = context.getCacheFiles();
            fis = fs.open(new Path(cacheFiles[0]));

            //2.读取数据 --- 一行一行的读取数据
            br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
            String line;
            while ((line = br.readLine()) != null) {
                //切割内容
                String[] split = line.split("\t");
                //3.将数据存放到map中
                map.put(split[0], split[1]);
            }

        }catch (Exception e) {
           e.printStackTrace();
        }finally {
            //4.关资源
            if (fs != null){
                fs.close();
            }
            if (br != null){
                br.close();
            }
            if (fis != null){
                fis.close();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.切割数据
        String[] orderInfo = value.toString().split("\t");
        //2.封装K,V
        OrderBean outKey = new OrderBean();
        //2.1封装主表数据
        outKey.setId(orderInfo[0]);
        outKey.setPid(orderInfo[1]);
        outKey.setAmount(orderInfo[2]);
        //2.2封装缓存内的附表数据
        outKey.setPname(map.get(orderInfo[1]));
        //3.将数据写出去
        context.write(outKey,NullWritable.get());
    }
}
