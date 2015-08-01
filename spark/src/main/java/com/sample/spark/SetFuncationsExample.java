package com.sample.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SetFuncationsExample {

	public static void main(String[] args) {
	SparkConf conf = new SparkConf();
	conf.setAppName("setfunctionexample");
	conf.setMaster("spark://rlab:7077");

	JavaSparkContext sc = new JavaSparkContext(conf);
	JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("bhuvan","panday","shashi","shyamu"));
	JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("dev","Nanja","bhuvan","shyamu"));
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	System.out.println("Union is :" + rdd1.union(rdd2).collect());
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	System.out.println("Intersecation is:" + rdd1.intersection(rdd2).collect() );
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	System.out.println("Difference rrd1 -rdd2: " + rdd1.subtract(rdd2).collect());
	System.out.println("Difference rrd1 -rdd2: " + rdd2.subtract(rdd1).collect());
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	System.out.println("Cartisian Product is: " + rdd1.cartesian(rdd2).collect());
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	sc.close();
	
	}
}
