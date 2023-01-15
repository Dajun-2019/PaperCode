package org.Demo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class Dataset {
    public static class Person implements Serializable{
        private String name;
        private long age;

        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }

        public long getAge() {
            return age;
        }
        public void setAge(long age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        SparkSession spark = SparkSession
                .builder()
                .appName("Demo02")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // TODO:Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        org.apache.spark.sql.Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();


        // TODO:Encoders for most common types are provided in class Encoders
        Encoder<Long> longEncoder = Encoders.LONG();
        org.apache.spark.sql.Dataset<Long> primitiveDS = spark.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
        org.apache.spark.sql.Dataset<Long> transformedDS = primitiveDS.map(
                (MapFunction<Long, Long>) value -> value + 1L,
                longEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]


        // TODO:DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "people.json";
        org.apache.spark.sql.Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
    }
}
