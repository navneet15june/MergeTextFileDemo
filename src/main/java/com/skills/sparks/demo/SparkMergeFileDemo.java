package com.skills.sparks.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkMergeFileDemo {

    private static JavaSparkContext sc;

    public SparkMergeFileDemo(JavaSparkContext sc){
        this.sc = sc;
    }

    public static void main(String[] args) {

        //Read two files and  convert them to RDD
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));
        SparkMergeFileDemo job = new SparkMergeFileDemo(sc);

        JavaRDD<String> rddEmpName = sc.textFile("E:\\EmployeeNames.csv");
        JavaRDD<String>  rddEmpSalary = sc.textFile("E:\\EmployeeSalary.csv");
        JavaRDD<String>  rddEmpManager = sc.textFile("E:\\EmployeeManager.csv");

        JavaPairRDD<String, String> pairRddEmpName = getPairRddBySplittingRecord(rddEmpName);
        JavaPairRDD<String, String> pairRddEmpSalary = getPairRddBySplittingRecord(rddEmpSalary);
        JavaPairRDD<String, String> pairRddEmpManager = getPairRddBySplittingRecord(rddEmpManager);

        JavaPairRDD<String, Tuple2<String, String>> empNameAndSalaryJoin = pairRddEmpName.join(pairRddEmpSalary);
        JavaPairRDD<String, Tuple2<Tuple2<String, String>, String>> join = empNameAndSalaryJoin.join(pairRddEmpManager);

        JavaPairRDD<String, Tuple2<Tuple2<String, String>, String>> sortedRdd = join.sortByKey();
        // Below line will have records like (E01,((Lokesh,50000),Vishnu)), (E02,((Bhupesh,50000),Satyam))...
        System.out.println(sortedRdd.collect());

        // Converting E01,((Lokesh,50000),Vishnu)) -> E01,Lokesh,50000,Vishnu
        JavaRDD<String> outputRDD = sortedRdd.map(new Function<Tuple2<String, Tuple2<Tuple2<String, String>, String>>, String>() {
            public String call(Tuple2<String, Tuple2<Tuple2<String, String>, String>> v) {
                return new String(v._1 + "," + v._2._1._1 + "," + v._2._1._2 + "," + v._2._2);
            }
        });

        outputRDD.saveAsTextFile("E:\\output\\demo\\");
    }

    public static JavaPairRDD<String, String> getPairRddBySplittingRecord(JavaRDD<String> p) {
        return p.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] keyAndValue = s.split(",");
                return new Tuple2<String, String>(keyAndValue[0], keyAndValue[1]);
            }
        });
    }
}
