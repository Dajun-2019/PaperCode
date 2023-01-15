package org.Demo;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;

/**
 * Spark SQL
 * @author dajunnnnnn
 * @Date 2022.11.06
 */
public class SparkSQL
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        //DataFrames
        Dataset<Row> df = spark.read().json("people.json");

        // Displays the content of the DataFrame to stdout
        df.show();

        // Print the schema in a tree format
        df.printSchema();

        // Select only the "name" column
        df.select("name").show();

        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();

        // Select people older than 21
        df.filter(col("age").gt(21)).show();

        // Count people by age
        df.groupBy("age").count().show();

        // TODO: Global Temporary View
        // Register the DataFrame as a global temporary view
        try {
            df.createGlobalTempView("people");
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();

    }
}
