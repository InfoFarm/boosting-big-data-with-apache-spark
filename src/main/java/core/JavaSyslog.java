package core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class JavaSyslog {

    public static void main(String[] args) {

        JavaSparkContext sparkContext = new JavaSparkContext("local[8]", "JavaSyslog");

        JavaRDD<Integer> data = sparkContext.textFile("/var/log/syslog")
                                            .map(String::length)
                                            .distinct();

        JavaPairRDD<Integer, Iterable<Integer>> first = data.groupBy(size -> size % 10);
        JavaPairRDD<Integer, Integer> second = data.map(size -> size + 1)
                                                   .filter(size -> size % 2 == 0)
                                                   .mapToPair(size -> new Tuple2<>(size % 10, size * size));

        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Integer>> joined = first.join(second);

        joined.take(15).forEach(System.out::println);
    }
}