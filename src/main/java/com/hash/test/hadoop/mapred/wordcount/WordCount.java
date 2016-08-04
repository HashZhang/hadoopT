package com.hash.test.hadoop.mapred.wordcount;

import com.hash.test.hadoop.hdfs.HDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/8/3
 */
public class WordCount extends Configured implements Tool {


    public int run(String[] strings) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "sfdba");

        Job job = Job.getInstance();
        job.setJar("D:\\Users\\862911\\hadoopT\\target\\hadoopT-1.0-SNAPSHOT.jar");
        //设置Job名称
        job.setJobName("My Word Count");
        //设置每阶段输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置Map的类和reduce的类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        //设置输入输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输入输出路径，远程Hdfs需要加链接地址
        FileInputFormat.setInputPaths(job, strings[0]);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        //先删除输出目录
        HDFS.deleteDir(strings[1]);

        final boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WordCount(),args);
        System.exit(ret);
    }
}

class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        while (stringTokenizer.hasMoreTokens()) {
            word.set(stringTokenizer.nextToken());
            context.write(word, one);
        }
    }
}

class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
