package org.openwest.spark.demo;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openwest.spark.model.Person;

import scala.Tuple2;

public class RDDDemo {
    public static void main( String[ ] args ) {
        System.setProperty( "hadoop.home.dir", "C:/Users/hrputhal/Downloads/hadoop-winutils-2.6.0" );
        SparkConf conf = new SparkConf( );
        conf.setAppName( "RDDDemo" ).setMaster( "local[*]" );
        SparkContext sc = new SparkContext( conf );
        JavaRDD< String > inputRDD = sc.textFile( "src/main/resources/us-500.csv", 3 ).toJavaRDD( );
        JavaRDD< Person > peopleRDD = inputRDD.map( line -> line.split( "," ) ).map(
                p -> new Person( p[ 0 ], p[ 1 ], p[ 2 ], p[ 3 ], p[ 4 ], p[ 5 ], p[ 6 ], new Long( p[ 7 ].trim( ) ) ) );
        JavaPairRDD< String, Integer > stateToOneRDD = peopleRDD
                .mapToPair( p -> new Tuple2< String, Integer >( p.getState( ), 1 ) );
        JavaPairRDD< String, Integer > countsRDD = stateToOneRDD.reduceByKey( ( Integer a, Integer b ) -> a + b );
        Map< String, Integer > collectAsMap = countsRDD.collectAsMap( );
        collectAsMap.forEach( ( String state, Integer count ) -> System.out.println( state + "----" + count ) );
    }
}
