package zhangchao.bayes.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

/**
 * 虽然我的输入目录是hdfs://hadoop01:9000/Country，下面有三个子目录AUSTR，BRAZ，CANA
 * 但是通过看日志，我发现这三个目录给了3个不同的进程来处理,每个RecordReader处理的目录是
 * hdfs://hadoop01:9000/Country/AUSTR，
 * hdfs://hadoop01:9000/Country/BRAZ，
 * hdfs://hadoop01:9000/Country/CANA
 */
public class ClassNameRecordReader extends RecordReader<Text, IntWritable> {
    //存放当前分类
    private String className;
    //目录里面的path文件
    private Path[] files;
    //当前处理到第几个文件了
    private int filePos = 0;
    //过程
    private float start = 0;
    //kv
    private Text key;
    private IntWritable value;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        //输入的目录
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        //看日志，自动被分成了三块
        //hdfs://hadoop01:9000/Country/AUSTR
        //hdfs://hadoop01:9000/Country/BRAZ
        //hdfs://hadoop01:9000/Country/CANA
        System.out.println("the xxxxxxxxxxxxxxxxpath=" + path.toString());
        className = path.getName();
        //获得子目录，即是目录下的所有文件
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] statuses = fs.listStatus(path);
        files = FileUtil.stat2Paths(statuses);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (filePos >= files.length) {
            return false;
        }
        if (null == key) {
            key = new Text();
        }
        if (null == value) {
            value = new IntWritable();
        }
        //设置key
        key.set(className);
        //设置value
        value.set(1);
        filePos++;
        start++;
        return true;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return start / files.length;
    }

    public void close() throws IOException {

    }
}
