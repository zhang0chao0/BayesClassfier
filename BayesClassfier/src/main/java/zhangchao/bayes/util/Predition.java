package zhangchao.bayes.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

/**
 * 预测的工具类
 */
public class Predition {
    //先验概率
    private static Map<String, Double> priors;
    //条件概率 <<类别,单词>,概率>
    private static Map<String, Double> termProbability;
    //训练集中单词集合V
    private static Set<String> termSet;
    //训练集中三类数据的单词个数
    private static Map<String, Double> classTermSum;


    public static Map<String, Double> getPriors() {
        return priors;
    }

    public static Map<String, Double> getTermProbability() {
        return termProbability;
    }

    public static Set<String> getTermSet() {
        return termSet;
    }

    public static Map<String, Double> getClassTermSum() {
        return classTermSum;
    }

    static {
        //当类被调用的时候计算概率，并保存到类成员变量中
        try {
            calPriors();
            calTermProbability();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //计算先验概率，注意加log()
    private static void calPriors() throws IOException {
        priors = new HashMap<String, Double>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(Constant.CLASS_SUM_LOC), conf);
        InputStream in = null;
        List<String> classNames = new ArrayList<String>();
        List<Integer> classSums = new ArrayList<Integer>();
        //double的原因是为了除法的时候自动转double
        double sum = 0.0;
        try {
            in = fs.open(new Path(Constant.CLASS_SUM_LOC));
            Scanner scanner = new Scanner(in);
            while (scanner.hasNext()) {
                String className = scanner.next();
                int classSum = scanner.nextInt();
                classNames.add(className);
                classSums.add(classSum);
                sum += classSum;
            }
            scanner.close();
        } finally {
            if (null != in) {
                in.close();
            }
        }
        for (int i = 0; i < classNames.size(); i++) {
            priors.put(classNames.get(i), Math.log(classSums.get(i) / sum));
        }
        //System.out.println(priors.toString());
    }

    //计算每个单词出现的概率
    //最后计算结果，<<类别,单词>,概率>
    private static void calTermProbability() throws IOException {
        termProbability = new HashMap<String, Double>();
        //统计每个类有多少单词，统计全部类的集合数量
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(Constant.TERM_LOC), conf);
        InputStream in = null;
        Map<String, Integer> terms = new HashMap<String, Integer>();
        //保存V集合，用set保证不重复
        termSet = new HashSet<String>();
        classTermSum = new HashMap<String, Double>();
        double austr = 0.0;
        double braz = 0.0;
        double cana = 0.0;
        try {
            in = fs.open(new Path(Constant.TERM_LOC));
            Scanner scanner = new Scanner(in);
            while (scanner.hasNext()) {
                String termName = scanner.next();
                int termSum = scanner.nextInt();
                String[] split1 = termName.split(",");
                if (split1[0].equals(Constant.AUSTR)) {
                    austr += termSum;
                } else if (split1[0].equals(Constant.BRAZ)) {
                    braz += termSum;
                } else {
                    cana += termSum;
                }
                termSet.add(split1[1]);
                terms.put(termName, termSum);
            }
            scanner.close();
        } finally {
            if (null != in) {
                in.close();
            }
        }
        //统计次数
        classTermSum.put(Constant.AUSTR, austr);
        classTermSum.put(Constant.BRAZ, braz);
        classTermSum.put(Constant.CANA, cana);
        //三类中所有的单词集合都保存在termSet中，不重复
        int V_len = termSet.size();
        for (String s : termSet) {
            String key1 = Constant.AUSTR + "," + s;
            //该单词属于AUSTR的概率 平滑操作+1 如果该单词在AUSTR类中不存在，则为0
            int num1 = 0;
            if (terms.get(key1) != null) {
                num1 = terms.get(key1);
            }
            double p1 = (num1 + 1) / (austr + V_len);
            termProbability.put(key1, Math.log(p1));
            //同理，BRAZ
            String key2 = Constant.BRAZ + "," + s;
            int num2 = 0;
            if (terms.get(key2) != null) {
                num2 = terms.get(key2);
            }
            double p2 = (num2 + 1) / (braz + V_len);
            termProbability.put(key2, Math.log(p2));
            //同理，CANA
            String key3 = Constant.CANA + "," + s;
            int num3 = 0;
            if (terms.get(key3) != null) {
                num3 = terms.get(key3);
            }
            double p3 = (num3 + 1) / (cana + V_len);
            termProbability.put(key3, Math.log(p3));
        }
    }

    public static void main(String[] args) throws Exception {

    }
}
