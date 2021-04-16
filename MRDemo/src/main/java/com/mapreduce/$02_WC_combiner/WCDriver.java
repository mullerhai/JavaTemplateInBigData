package com.mapreduce.$02_WC_combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    Combiner过程，发生在每次map将数据排序后，将map输出的数据先局部"reduce"
    Combiner继承自Reducer类，重写reduce方法，其代码完全相同
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //设置Combiner
        job.setCombinerClass(WCCombiner.class);

        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\The Old Man And the Sea.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\output"));
        job.waitForCompletion(true);
    }
}
/*
 * InputFormat(combine) -> Mapper ->shuffle(sort combiner) -> Reducer -> OutputFormat(MyOutputFormat)
 *
 * WordCount+combine(InputFormat合并小文件)+combiner(局部汇总,直接copy Reducer代码)
 *
 * FlowBean(序列化实现Writable)+comparable(实现流量降序排列)+partition(按手机号前三位分区导出)
 *
 * OutputFormat(自定义输出文件名，格式，数量，需要自定义OutputFormat类，RecordWriter类)
 * 没有Mapper和Reducer类，从inputFormat直接到OutputFormat，含有atguigu的网址输出到一个txt，其余一个txt
 *
 * MapJoin（增加缓存文件） & ReduceJoin（自定义分组 == 自定义排序）
 *
 * ETL数据清洗（只含mapper，去除较短log信息）
 * */