package www.kgc.cn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by kgc on 2017/11/28.
 */
public class SimpleApp {
    public static void main(String[] args){
        String logFile = "/home/hadoop/app/spark-1.6.3-bin-hadoop2.6/README.md";
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile);

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a:" + numAs + ", lines with b:" + numBs);
    }
}
