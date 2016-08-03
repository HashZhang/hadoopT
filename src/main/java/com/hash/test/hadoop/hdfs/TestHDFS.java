package com.hash.test.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/20
 */
public class TestHDFS {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://nosql1:9000/";
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);

        // 列出hdfs上/user/fkong/目录下的所有文件和目录
        FileStatus[] statuses = fs.listStatus(new Path("/test/input"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }

        // 在hdfs的/user/fkong目录下创建一个文件，并写入一行文本
//        FSDataOutputStream os = fs.create(new Path("/remoteTest/input/test.log"));
//        os.write("Hello World!".getBytes());
//        os.flush();
//        os.close();

        // 显示在hdfs的/user/fkong下指定文件的内容
        InputStream is = fs.open(new Path("/test/output/part-r-00000"));
        IOUtils.copyBytes(is, System.out, 1024, true);
    }
}
