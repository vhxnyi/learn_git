package com.big.data.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FlowCountTask {

    public static final String INPUT_PATH = "hdfs://localhost:9000/geek-big-data-camp/second-week/mapreduce/input";
    public static final String OUTPUT_PATH = "hdfs://localhost:9000/geek-big-data-camp/second-week/mapreduce/output";

    public static void main(String[] args) throws Exception {
        // 0 初始化任务
        Job job = new Job(new Configuration(), FlowCountTask.class.getSimpleName());

        // 1.1 指定输入文件路径
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        // 1.2 指定哪个类用来格式化输入文件
        job.setInputFormatClass(TextInputFormat.class);

        // 2.1 指定自定义的Mapper类
        job.setMapperClass(MyMapper.class);
        // 2.2 指定输出<k2,v2>的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(KpiWritable.class);

        // 2.3 指定分区类
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        // 2.4 todo 排序(最近疯狂加班，来不及做了，先提交部分逻辑，后续补上)
        // job.setSortComparatorClass();

        // 3.1 指定自定义的Reduce类
        job.setReducerClass(MyReducer.class);
        // 3.2 指定输出<k3,v3>的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(KpiWritable.class);

        // 4.3 指定输出到哪里
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        // 4.4 设定输出文件的格式化类
        job.setOutputFormatClass(TextOutputFormat.class);

        // 5 把代码提交给JobTracker执行
        job.waitForCompletion(true);
    }


    static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable> {

        protected void map(
                LongWritable key,
                Text value,
                org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, KpiWritable>.Context context)
                throws IOException, InterruptedException {

            final String[] splitArr = value.toString().split("\t");
            final String msisdn = splitArr[1];
            final Text k2 = new Text(msisdn);
            final KpiWritable v2 = new KpiWritable(splitArr[6], splitArr[7],
                    splitArr[8], splitArr[9]);

            context.write(k2, v2);
        }
    }

    static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable> {

        /**
         * @param k2  表示整个文件中不同的手机号码
         * @param v2s 表示该手机号在不同时段的流量的集合
         */
        protected void reduce(
                Text k2,
                java.lang.Iterable<KpiWritable> v2s,
                org.apache.hadoop.mapreduce.Reducer<Text, KpiWritable, Text, KpiWritable>.Context context)
                throws IOException, InterruptedException {

            long upPackNum = 0L;
            long downPackNum = 0L;
            long upPayLoad = 0L;
            long downPayLoad = 0L;

            for (KpiWritable kpiWritable : v2s) {
                upPackNum += kpiWritable.upPackNum;
                downPackNum += kpiWritable.downPackNum;
                upPayLoad += kpiWritable.upPayLoad;
                downPayLoad += kpiWritable.downPayLoad;
            }

            final KpiWritable v3 = new KpiWritable(upPackNum + "", downPackNum + "",
                    upPayLoad + "", downPayLoad + "");

            context.write(k2, v3);
        }
    }
}




