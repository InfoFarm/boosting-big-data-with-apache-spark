package core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class JavaSample {

    public static void main(String[] args) {

        /*
         * We use a Java-specific version of the SparkContext
         */
        JavaSparkContext sparkContext = new JavaSparkContext("local[4]", "core.JavaSample");

        /*
         * We create an RDD from a local list of Strings
         */
        JavaRDD<String> lines = sparkContext.parallelize(Arrays.asList("hello world", "it's Java"));

        /*
         * Split the lines of text to words
         */
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")));


        /*
         * Fetch the first word
         */
        System.out.println(words.first());
    }
}
