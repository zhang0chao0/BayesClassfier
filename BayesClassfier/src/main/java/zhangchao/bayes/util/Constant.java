package zhangchao.bayes.util;

public class Constant {
    //map reduce的输入输出
    public static final String CLASS_SUM_MAP_INPUT = "hdfs://hadoop01:9000/Country";
    public static final String CLASS_SUM_REDUCE_OUTPUT = "hdfs://hadoop01:9000/Bayes/ClassSum";
    public static final String TERM_REDUCE_OUTPUT = "hdfs://hadoop01:9000/Bayes/Term";
    public static final String PREDICT_MAP_INPUT = "hdfs://hadoop01:9000/Test";
    public static final String PREDICT_REDUCE_OUTPUT = "hdfs://hadoop01:9000/Bayes/Predict";
    //计算的结果文件
    public static final String CLASS_SUM_LOC = "hdfs://hadoop01:9000/Bayes/ClassSum/part-r-00000";
    public static final String TERM_LOC = "hdfs://hadoop01:9000/Bayes/Term/part-r-00000";
    //类别
    public static final String AUSTR = "AUSTR";
    public static final String BRAZ = "BRAZ";
    public static final String CANA = "CANA";
    public static final String[] CLASS_ARRAY = {"AUSTR", "BRAZ", "CANA"};
}
