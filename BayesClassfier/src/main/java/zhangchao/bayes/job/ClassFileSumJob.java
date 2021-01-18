package zhangchao.bayes.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import zhangchao.bayes.input.ClassNameInputFormat;
import zhangchao.bayes.util.Constant;

import java.io.IOException;


/**
 * 第一个MapReduce Job，统计每个类的文件总数，用于先验概率
 * 输出<类名，文档总数>
 */
public class ClassFileSumJob {
    //由于InputFormat做好了，Map不做任何处理
    public static class ClassFileSumMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    //Reducer做相加处理
    public static class ClassFileSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ClassFileSum");
        job.setJarByClass(ClassFileSumJob.class);
        job.setMapperClass(ClassFileSumMapper.class);
        job.setReducerClass(ClassFileSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //自定义InputFormat
        job.setInputFormatClass(ClassNameInputFormat.class);
        //输入目录
        FileInputFormat.addInputPath(job, new Path(Constant.CLASS_SUM_MAP_INPUT));
        //输出目录
        FileOutputFormat.setOutputPath(job, new Path(Constant.CLASS_SUM_REDUCE_OUTPUT));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
