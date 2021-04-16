package com.hadoop.hdfs;

/*
    HDFS API操作：
    1.创建文件系统对象
    2.具体操作 ：上传，删除，下载.....
    3.关资源
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSDemo {

    private FileSystem fs;

    // 创建文件系统对象
    @Before
    public void before() throws Exception {

        // uri : HDFS的地址(NameNode地址)
        URI uri = new URI("hdfs://hadoop102:9820");
        //conf : 需要使用的配置信息
        Configuration conf = new Configuration();
        //user : 用来操作HDFS的用户
        String user = "YOUR_USERNAME";
        fs = FileSystem.get(uri, conf, user);

    }

    // 关闭资源
    @After
    public void after(){
        try {
            if (fs != null) {
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //从本地文件系统上传到HDFS
    @Test
    public void upload() throws IOException {
        /*
        copyFromLocalFile(boolean delSrc, boolean overwrite,Path src, Path dst)
          delSrc : 是否删除源文件
          overwrite : 如果目标文件已存在是否覆盖目标文件
                注意：如果不覆盖但目标文件又存在则报错
          src : 源文件路径(本地)
          dst : 目标文件路径(HDFS)
          注意：dst目标路径如果是个文件夹则传到文件夹中，如果文件夹不存在则创建一个同名文件传入信息
         */
        fs.copyFromLocalFile(false, true,
                new Path("E:\\input\\log.txt"),new Path("/HDFSDemo"));
    }

    //从HDFS下载到本地
    @Test
    public void download() throws IOException {
        /*
        copyToLocalFile(boolean delSrc, Path src, Path dst,boolean useRawLocalFileSystem)
        delSrc : 是否删除源文件(HDFS上的文件)
        src : 源文件路径(HDFS)
        dst : 目标文件路径(本地)
        useRawLocalFileSystem ： 是否使用useRawLocalFileSystem
                如果使用：不会下载crc校验文件
                如果不使用 ： 会下载crc校验文件
         */
        fs.copyToLocalFile(false,new Path("/HDFSDemo/log.txt"),
                new Path("E:\\"), true);

    }

    // 删除
    @Test
    public void delete() throws IOException {
        /*
            delete(Path f, boolean recursive)
            f : 删除的数据的路径
            recursive : 是否递归删除(删除非空文件夹会报错，必须递归删除)
         */
        fs.delete(new Path("/HDFSDemo/log.txt"),true);
    }

    // 改名
    @Test
    public void rename() throws IOException {
        /*
            rename(Path src, Path dst)
            src : 源文件路径
            dst : 目标文件路径
         */
        //fs.rename(new Path("/hdfs/test.txt"),new Path("/hdfs/new test.txt"));

        //移动(两路径不同则移动，相同则改名)
        fs.rename(new Path("/HDFSDemo/log.txt"),new Path("/log.txt"));
    }

    // 文件详情查看
    @Test
    public void getInfo() throws IOException {
        /*
            listFiles(final Path f, final boolean recursive)
            f : 目标路径
            recursive ： 是否递归
         */
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/HDFSDemo/log.txt"), true);

        while(remoteIterator.hasNext()){
            //文件详情对象
            LocatedFileStatus fileStatus = remoteIterator.next();
            //文件信息
            System.out.println("=====文件名:" + fileStatus.getPath().getName());
            System.out.println("=====所属主:" + fileStatus.getOwner());
            System.out.println("=====副本数量：" + fileStatus.getReplication());
            //获取块的信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation block : blockLocations) {
                System.out.println(block);
            }
        }
    }

    // 判断该目录下的是文件还是目录
    @Test
    public void isFileOrDir() throws IOException {
        /*
            参数:目标路径
         */
        FileStatus[] fileStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : fileStatus) {
            //输出文件名
            System.out.println("====" + status.getPath().getName() + "====");
            if (status.isDirectory()){
                System.out.println("是一个目录");
            }else if (status.isFile()){
                System.out.println("是一个文件");
            }
        }
    }

    // 用流的方式实现HDFS上传和下载
    @Test
    public void streamUpload() throws IOException {
        //读-从本地读
        FileInputStream fis = new FileInputStream("E:\\input\\test.txt");
        //写-向HDFS写
        FSDataOutputStream fos = fs.create(new Path("/HDFSDemo/log.txt"));

        /*
            文件对拷
            copyBytes(InputStream in, OutputStream out,int buffSize, boolean close)
             in : 输入流
             out : 输出流
             buffSize ：缓存大小
             close : 是否关流
         */
        //一边读一边写
        IOUtils.copyBytes(fis,fos,1024,false);

        //关流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    @Test
    public void steamDownload() throws IOException {
        //读 - 从HDFS上读
        FSDataInputStream fis = fs.open(new Path("/HDFSDemo/log.txt"));
        //写 - 向本地写
        FileOutputStream fos = new FileOutputStream("E:\\test.txt");
        //文件对拷
        IOUtils.copyBytes(fis,fos,1024,true);
    }
}











