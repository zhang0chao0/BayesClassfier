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
import zhangchao.bayes.util.Predition;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * 第三个Job
 * 计算输出<文件名,类名>
 */
public class PreditionJob {
    //先验概率
    private static Map<String, Double> priors = Predition.getPriors();
    //条件概率 <<类别,单词>,概率>
    private static Map<String, Double> termProbability = Predition.getTermProbability();
    //训练集中单词集合
    private static Set<String> termSet = Predition.getTermSet();
    //训练集中三类数据的单词个数
    private static Map<String, Double> classTermSum = Predition.getClassTermSum();


    /**
     * map输入<文件名,类名@term1,term2,term3,>
     * 输出<文件名,<类名,概率>>
     */
    public static class PreditionMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] split1 = value.toString().split("@");
            //当前类名 其实用不到
            //String currentClassName = split1[0];
            for (String className : Constant.CLASS_ARRAY) {
                //当前单词组
                String[] split2 = split1[1].split(",");
                //先验概率
                double prior = priors.get(className);
                for (String s : split2) {
                    //求文档属于该类的概率
                    //公式 = 先验概率 + 每个单词的概率
                    double temp = 0.0;
                    if (!termSet.contains(s)) {
                        //若单词不在训练的集合中
                        temp = Math.log(1 / (classTermSum.get(className) + termSet.size()));
                    } else {
                        temp = termProbability.get(className + "," + s);
                    }
                    prior += temp;
                }
                String v = className + "," + prior;
                context.write(key, new Text(v));
            }
        }
    }

    /**
     * 输入<文件名,{<类名1,概率1>,<类名2,概率2>,<类名3,概率3>}>
     * 输出<文件名，类名>，求最大值
     */
    public static class PreditionReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String maxClass = "";
            double maxPro = -999999.0;
            //System.out.println("this is reduce....................");
            for (Text value : values) {
                String className = value.toString().split(",")[0];
                double pro = Double.valueOf(value.toString().split(",")[1]);
                //System.out.println("this is reduce....................className=" + className);
                //System.out.println("this is reduce....................pro=" + pro);
                if (pro > maxPro) {
                    maxClass = className;
                    maxPro = pro;
                }
            }
            context.write(key, new Text(maxClass));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Predition");
        job.setJarByClass(PreditionJob.class);
        job.setMapperClass(PreditionJob.PreditionMapper.class);
        job.setReducerClass(PreditionJob.PreditionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //和Job2的InputFormat是一样的
        job.setInputFormatClass(TermStatisticInputFormat.class);
        //输入目录
        FileInputFormat.addInputPath(job, new Path(Constant.PREDICT_MAP_INPUT));
        //输出目录
        FileOutputFormat.setOutputPath(job, new Path(Constant.PREDICT_REDUCE_OUTPUT));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
