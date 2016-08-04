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
        // 列出hdfs上/user/fkong/目录下的所有文件和目录
        FileStatus[] statuses = HDFS.fs.listStatus(new Path("/test/input"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }

        // 显示在hdfs的/user/fkong下指定文件的内容
        InputStream is = HDFS.fs.open(new Path("/test/output/part-r-00000"));
        IOUtils.copyBytes(is, System.out, 1024, true);
    }
}
