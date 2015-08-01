package com.sample.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ReduceSunExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Sum using Reduce");
		conf.setMaster("spark://rlab:7077");
		
		JavaSparkContext sc  = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6));
		System.out.println("!!!!!!!!!!!!!!!!!!!!!");
		System.out.println("Sum of rdd is: " +  rdd1.reduce(new SumReduce()));
		System.out.println("Sum of rdd using fold: " + rdd1.fold(0, new SumReduce()));
		sc.close();
				

	}

}
class SumReduce implements Function2<Integer, Integer, Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Integer call(Integer v1, Integer v2) throws Exception {
		
		return v1 + v2;
	}
	
}
