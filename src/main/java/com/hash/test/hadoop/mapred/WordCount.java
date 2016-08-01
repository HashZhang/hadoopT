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
public class WordCount {
    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
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
        JobConf jobConf = new JobConf(WordCount.class);
        jobConf.set("mapred.jar","D:\\Users\\862911\\hadoopT\\target\\hadoopT-1.0-SNAPSHOT.jar");
        System.setProperty("HADOOP_USER_NAME", "sfdba");
        jobConf.setJobName("My Word Count");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(Map.class);
        jobConf.setReducerClass(Reduce.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, args[0]);
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        deleteDir(jobConf, args[1]);

        JobClient.runJob(jobConf);
    }
}

class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
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

class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }
        outputCollector.collect(text, new IntWritable(sum));
    }
}
