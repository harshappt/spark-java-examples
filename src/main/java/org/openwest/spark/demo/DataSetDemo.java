package org.openwest.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.openwest.spark.model.Person;

public class DataSetDemo {
    public static void main( String[ ] args ) {
        System.setProperty( "hadoop.home.dir", "C:/Users/hrputhal/Downloads/hadoop-winutils-2.6.0" );
        SparkConf conf = new SparkConf( );
        conf.setAppName( "RDDDemo" ).setMaster( "local[*]" );
        SparkContext sc = new SparkContext( conf );
        JavaRDD< String > inputRDD = sc.textFile( "src/main/resources/us-500.csv", 3 ).toJavaRDD( );
        JavaRDD< Person > peopleRDD = inputRDD.map( line -> line.split( "," ) ).map(
                p -> new Person( p[ 0 ], p[ 1 ], p[ 2 ], p[ 3 ], p[ 4 ], p[ 5 ], p[ 6 ], new Long( p[ 7 ].trim( ) ) ) );
        SQLContext sctxt = new SQLContext( sc );
        Dataset< Person > peopleDS = sctxt.createDataset( peopleRDD.collect( ), Encoders.bean( Person.class ) );
        peopleDS.groupBy( new Column( "state" ) ).count( ).printSchema( );
        SparkSession sparkSession = SparkSession.builder( ).appName( "DataFrames Demo" ).master( "local[*]" )
                .getOrCreate( );
    }
}
