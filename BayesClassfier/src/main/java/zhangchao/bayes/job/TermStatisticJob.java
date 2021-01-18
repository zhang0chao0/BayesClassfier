package zhangchao.bayes.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import zhangchao.bayes.input.TermStatisticInputFormat;
import zhangchao.bayes.util.Constant;

import java.io.IOException;

/**
 * 第二个Job
 * 统计单词的出现次数，输出
 * <类名,单词 出现次数>
 */
public class TermStatisticJob {
    /**
     * map输入<文件名,类名@term1,term2,term3,>
     * 输出<<类名,term1>,1>,<<类名,term2>,1>,<<类名,term3>,1>
     */
    public static class TermStatisticMapper extends Mapper<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable(1);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] split1 = value.toString().split("@");
            String className = split1[0];
            String[] split2 = split1[1].split(",");
            for (String s : split2) {
                String newKey = className + "," + s;
                context.write(new Text(newKey), result);
            }
        }
    }

    /**
     * reduce输入<newKey,{1,1,1,1}>
     * 输出<newKey,result>
     */
    public static class TermStatisticReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        Job job = Job.getInstance(conf, "TermStatistic");
        job.setJarByClass(TermStatisticJob.class);
        job.setMapperClass(TermStatisticJob.TermStatisticMapper.class);
        job.setReducerClass(TermStatisticJob.TermStatisticReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //自定义InputFormat
        job.setInputFormatClass(TermStatisticInputFormat.class);
        //输入目录
        FileInputFormat.addInputPath(job, new Path(Constant.CLASS_SUM_MAP_INPUT));
        //输出目录
        FileOutputFormat.setOutputPath(job, new Path(Constant.TERM_REDUCE_OUTPUT));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
