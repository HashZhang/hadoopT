package com.hash.test.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/8/4
 */
public class HDFS {
    private static String uri = "hdfs://nosql1:9000/";
    private static Configuration config = new Configuration();
    public static FileSystem fs;
    static {
        try {
            fs = FileSystem.get(URI.create(uri), config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteDir(String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path targetPath = new Path(dirPath);
        //如果文件夹存在，则删除
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has been deleted sucessfullly.");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }
    }

}
