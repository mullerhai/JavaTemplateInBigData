package com.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 处理压缩文件（压缩或者解压）都需要压缩流
 * 压缩流都需要先获取编码器对象codec
 *  1. 通过反射获取
 *  2. 通过工厂类获取
 * 由编码器对象create输入流(解压)或输出流(压缩)
 *  1. codec.createInputStream()
 *  2. codec.createOutputStream()
 *
 *  除了压缩流的另一个流使用普通的文件输入输出流即可
 *  两个流都准备好后直接调用IOUtils的文件对拷方法
 *  IOUtils.copyBytes(is,os,1024,true);
 */
public class CompressDemo {
    /*
        压缩（需要事先知道压缩文件格式）
     */
    @Test
    public void test() throws Exception {
        //1.输入流 -- 文件流
        FileInputStream fis = new FileInputStream("E:\\input\\web.log");
        //2.输出流 -- 压缩流
        //2.1通过反射工具类创建GzipCodec对象
        CompressionCodec gzipCodec = ReflectionUtils.newInstance(GzipCodec.class, new Configuration());
        //2.2创建CompressionOutputStream流的对象
        //getDefaultExtension() : 获取压缩的扩展名
        CompressionOutputStream cos = gzipCodec.createOutputStream(
                new FileOutputStream("E:\\input\\web.log" + gzipCodec.getDefaultExtension()));
        //3.文件对拷
        IOUtils.copyBytes(fis,cos,1024,true);
    }
    /*
         解压缩（需要事先知道压缩文件格式）
     */
    @Test
    public void test2() throws Exception {
        //1.输入流 --- 压缩流
        //1.1通过反射工具类创建GzipCodec对象
        CompressionCodec gzipCodec = ReflectionUtils.newInstance(GzipCodec.class, new Configuration());
        //1.2创建CompressionInputStream流的对象
        CompressionInputStream cis = gzipCodec.createInputStream(new FileInputStream("E:\\input\\web.log.gz"));
        //2.输出流 --- 文件流
        FileOutputStream fos = new FileOutputStream("E:\\input\\web.log");
        //3.文件对拷
        IOUtils.copyBytes(cis,fos,1024,true);
    }
     /*
        解压缩(使用工厂类)
     */
    @Test
    public void test3() throws IOException {
        //1.输入流 --- 压缩流
        //  1.1创建压缩类的工厂类 ---- 好处：可以根据文件的扩展名自动创建对应的类型类的对象
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path("E:\\input\\web.log.gz"));
        if (codec == null){//说明无法对该压缩类型创建对应的对象
            System.out.println("压缩类型有问题");
            return;
        }
        //  1.2创建流
        //  1.2创建CompressionInputStream流的对象
        CompressionInputStream is = codec.createInputStream(new FileInputStream("E:\\input\\web.log.gz"));
        //2.输出流 --- 文件流
        FileOutputStream fos = new FileOutputStream("E:\\input\\web.log");
        //3.文件对拷
        IOUtils.copyBytes(is,fos,1024,true);


    }

}
