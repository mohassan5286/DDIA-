package com.spark.ApacheSpark;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.spark.examples.sql;

// $example on:programmatic_schema$
import java.util.*;
// $example off:programmatic_schema$
// $example on:create_ds$
import java.io.Serializable;
// $example off:create_ds$

// $example on:schema_inferring$
// $example on:programmatic_schema$
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
// $example off:programmatic_schema$
// $example on:create_ds$
import org.apache.spark.api.java.function.MapFunction;
// $example on:create_df$
// $example on:run_sql$
// $example on:programmatic_schema$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off:programmatic_schema$
// $example off:create_df$
// $example off:run_sql$
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
// $example off:create_ds$
// $example off:schema_inferring$
import org.apache.spark.sql.RowFactory;
// $example on:init_session$
import org.apache.spark.sql.SparkSession;
// $example off:init_session$
// $example on:programmatic_schema$
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off:programmatic_schema$
import org.apache.spark.sql.AnalysisException;

// $example on:untyped_ops$
// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;
// $example off:untyped_ops$
//java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -jar target/original-spark-springboot.jar

public class SparkJavaApplication {

	public static void main(String[] args) throws AnalysisException {

		double startTime, endTime, avargeTimeSpark = 0, avargeTimeMySQL = 0;

		for(int i=0; i<8; i++)
		{
			// $example on:init_session$
			SparkSession spark = SparkSession
					.builder()
					.appName("Java Spark SQL basic example")
					.config("spark.some.config.option", "some-value")
					.getOrCreate();
			// $example off:init_session$

			System.out.println("Spark is running...\n");

			startTime = System.currentTimeMillis();
			executeSparkQuery(spark);
			endTime = System.currentTimeMillis();
			avargeTimeSpark += (endTime - startTime);

			spark.stop();
		}
		System.out.println("Time for Spark: " + avargeTimeSpark / 8 + " seconds");

		for(int i=0; i<8; i++) {
			startTime = System.currentTimeMillis();
			executeMySQLQuery();
			endTime = System.currentTimeMillis();
			avargeTimeMySQL += (endTime - startTime);
		}

		System.out.println("Time for MySQL: " + avargeTimeMySQL / 8 + " seconds");
//
	}

	private static void executeMySQLQuery() {
		
		Repository repository = null;
		
		repository.runQuery();
		
	}

	private static void executeSparkQuery(SparkSession spark) {

		Dataset<Row> df = spark.read().json("examples/src/main/resources/mtcars.parquet");

		df.createOrReplaceTempView("cars");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM cars");

//		sqlDF.show();

	}

}