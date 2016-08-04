package com.hash.test.hadoop.hdfs.eliminateduplicated;

import com.hash.test.hadoop.hdfs.HDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/8/4
 */
public class InitData {
    public static void main(String[] args) throws IOException {
        HDFS.deleteDir("/test/input/test.log");
        FSDataOutputStream os = HDFS.fs.create(new Path("/test/input/test.log"));
        BufferedReader bufferedReader = new BufferedReader(new FileReader(ClassLoader.getSystemResource("").toString().substring(6)+"/logsToBeCleaned/stdout.log"));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            os.write(line.getBytes());
        }
        os.flush();
        bufferedReader.close();
        os.close();
    }
}
