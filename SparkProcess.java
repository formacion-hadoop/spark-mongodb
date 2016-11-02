package com.formacionhadoop;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

import scala.Tuple2;

public class SparkProcess {

	static String fileName = "/tmp/clientesTienda";
	static String[] schema = { "cod", "nombre", "apellidos", "telefono"};

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkToMongoDB");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(fileName);

		JavaRDD<Map<String, ?>> fields = textFile
				.map(new Function<String, Map<String, ?>>() {
					private static final long serialVersionUID = 1L;

					public Map<String, ?> call(String line) throws Exception {
						Map<String, String> mongodbFields = new HashMap<String, String>();
						String fieldSplit[] = line.split(",");

						for (int i = 1; i < fieldSplit.length; i++) {
							mongodbFields.put(schema[i], fieldSplit[i]);
										
						}

						return mongodbFields;
					}
				});


		JavaPairRDD<String, HashMap<String, Object>> rddFields = textFile
				.mapToPair(new PairFunction<String, String, HashMap<String, Object>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, HashMap<String, Object>> call(String s) {
						HashMap<String, Object> values = new HashMap<String, Object>();
						String lineValues[] = s.split(",");
						String id = "";
						for (int i = 0; i < schema.length; i++) {
							if (i < lineValues.length) {
								values.put(schema[i], lineValues[i]);
							}
						}
						id = lineValues[0];
						return new Tuple2<String, HashMap<String, Object>>(id,
								values);
					}
				});
		
		
		JavaRDD<Document> mongodbDocuments = rddFields.values()
				.map(new Function<HashMap<String, Object>, Document>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Document call(HashMap<String, Object> arg0) throws Exception {
						Document mongodbDoc = new Document();
						mongodbDoc.putAll(arg0);
						return mongodbDoc;
					}

				});
		
		try {
			MongoSpark.save(mongodbDocuments, getConfig());
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		sc.close();

	}
		

	private static WriteConfig getConfig() {
		Map<String, String> configMongoDB = new HashMap<String, String>();
		configMongoDB.put("spark.mongodb.output.uri", "mongodb://localhost/test.spark");
		WriteConfig.create(configMongoDB);
		WriteConfig writeConfig = WriteConfig.create(configMongoDB);

		return writeConfig;
	}
	

}
