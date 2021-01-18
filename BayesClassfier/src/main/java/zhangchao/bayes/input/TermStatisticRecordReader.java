package zhangchao.bayes.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

public class TermStatisticRecordReader extends RecordReader<Text, Text> {
    //存放当前分类
    private String className;
    //目录里面的path文件
    private Path[] files;
    //当前处理到第几个文件了
    private int filePos = 0;
    //当前的文件系统
    private FileSystem fs;
    //过程
    private float start = 0;
    //kv
    private Text key;
    private Text value;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        FileSplit split = (FileSplit) inputSplit;
        //输入的目录，hdfs://hadoop01:9000/Country/AUSTR
        Path path = split.getPath();
        className = path.getName();
        fs = path.getFileSystem(conf);
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
            value = new Text();
        }
        //当前的所有文件目录都存在files数组里面
        Path currentFile = files[filePos];
        InputStream in = null;
        //保存一个key
        StringBuilder sb = new StringBuilder();
        sb.append(className + "@");
        try {
            in = fs.open(currentFile);
            Scanner scanner = new Scanner(in);
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                sb.append(line + ",");
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != in) {
                in.close();
            }
        }
        //key为当前的文件名
        key.set(currentFile.getName());
        value.set(sb.toString());
        filePos++;
        start++;
        return true;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return start / files.length;
    }

    public void close() throws IOException {

    }
}
