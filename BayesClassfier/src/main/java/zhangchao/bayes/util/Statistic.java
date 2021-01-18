package zhangchao.bayes.util;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Scanner;

//统计
public class Statistic {
    @Test
    public void test1() throws Exception {
        //将统计的文件简单的处理一下
        FileInputStream inputStream = new FileInputStream("F:/predict");
        FileOutputStream outputStream = new FileOutputStream("F:/predictPlus");
        PrintWriter pw = new PrintWriter(outputStream);
        Scanner in = new Scanner(inputStream);
        int i = 1;
        while (in.hasNext()) {
            String fileName = in.next();
            String className = in.next();
            if (i <= 76) {
                pw.print("AUSTR " + className);
            } else if (i > 76 && i <= 126) {
                pw.print("BRAZ " + className);
            } else {
                pw.print("CANA " + className);
            }
            i++;
            pw.print("\n");
        }
        in.close();
        pw.close();
        outputStream.close();
        inputStream.close();
    }

    @Test
    public void test2() throws Exception {
        //统计
        FileInputStream inputStream = new FileInputStream("F:/predictPlus");
        Scanner in = new Scanner(inputStream);
        //AUSTR BRAZ CANA
        String currentClass = "CANA";
        double TP = 0.0;
        double FN = 0.0;
        double FP = 0.0;
        double TN = 0.0;
        while (in.hasNext()) {
            String realClass = in.next();
            String predictClass = in.next();
            if (realClass.equals(currentClass) && predictClass.equals(currentClass)) {
                TP++;
            } else if (realClass.equals(currentClass) && !predictClass.equals(currentClass)) {
                FN++;
            } else if (!realClass.equals(currentClass) && predictClass.equals(currentClass)) {
                FP++;
            } else if (!realClass.equals(currentClass) && !predictClass.equals(currentClass)) {
                TN++;
            }
        }
        in.close();
        inputStream.close();
        double P = TP / (TP + FP);
        double R = TP / (TP + FN);
        double F1 = 2 * P * R / (P + R);
        System.out.println("TP=" + TP);
        System.out.println("FN=" + FN);
        System.out.println("FP=" + FP);
        System.out.println("TN=" + TN);
        System.out.println("---------------------------");
        System.out.println("P=" + P);
        System.out.println("R=" + R);
        System.out.println("F1=" + F1);
    }
}
