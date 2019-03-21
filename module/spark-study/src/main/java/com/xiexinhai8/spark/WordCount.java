package com.xiexinhai8.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
	private SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("word count");
	private JavaSparkContext sc = new JavaSparkContext(sparkConf);
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usge: Word Count <file>");
			System.exit(1);
		}
		String inputFile = args[0];
		String outputFile = args[1];
		WordCount wordCount = new WordCount();
		wordCount.compute(inputFile, outputFile);

	}

	public void compute(String inputFile, String outputFile) {
		JavaRDD<String> rdd = this.sc.textFile(inputFile);
		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				return (Iterator<String>) Arrays.asList(s.split(" "));
			}
		});

		JavaPairRDD<String, Integer> wordsPair = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> wordsCounts = wordsPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wordsCounts.saveAsTextFile(outputFile);

	}

}

