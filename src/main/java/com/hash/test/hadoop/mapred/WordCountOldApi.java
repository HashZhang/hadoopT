package com.hash.test.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/20
 */
public class WordCountOldApi {
    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
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

    public static void main(String[] args) throws IOException {
        //设置工作类，就是main方法所在类
        JobConf jobConf = new JobConf(WordCountOldApi.class);
        //配置需要运行的JAR在本地的位置，就是本类所在的JAR包
        jobConf.set("mapred.jar", "D:\\Users\\862911\\hadoopT\\target\\hadoopT-1.0-SNAPSHOT.jar");
        //远程Hadoop集群的用户，防止没有权限
        System.setProperty("HADOOP_USER_NAME", "sfdba");
        //设置Job名称
        jobConf.setJobName("My Word Count");
        //设置每阶段输出格式
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);
        //设置Map的类和reduce的类
        jobConf.setMapperClass(MapOldApi.class);
        jobConf.setReducerClass(ReduceOldApi.class);
        //设置输入输出格式
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        //设置输入输出路径，远程Hdfs需要加链接地址
        FileInputFormat.setInputPaths(jobConf, args[0]);
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
        //先删除输出目录
        deleteDir(jobConf, args[1]);
        //执行Job
        JobClient.runJob(jobConf);
    }
}

class MapOldApi extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private final Text key = new Text();

    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String line = text.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        while (stringTokenizer.hasMoreTokens()) {
            key.set(stringTokenizer.nextToken());
            outputCollector.collect(key, one);
        }
    }
}

class ReduceOldApi extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }
        outputCollector.collect(text, new IntWritable(sum));
    }
}
