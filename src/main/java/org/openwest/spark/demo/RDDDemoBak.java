package org.openwest.spark.demo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*
 * The driver is the process where the main() method of your program runs. 
 * It is the process running the user code that creates a SparkContext, creates RDDs, and performs transformations and actions
 *
 */
public class RDDDemoBak {
    public static void main( String[ ] args ) {
        SparkConf conf = new SparkConf( ).setAppName( "RDDDemo" ).setMaster( "local[*]" );
        JavaSparkContext sc = new JavaSparkContext( conf );
        /*
         * The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of
         * elements partitioned across the nodes of the cluster that can be operated on in parallel.
         */
        rddFromCollection( sc );
        /*
         * Spark can create distributed datasets from any storage source supported by Hadoop, including your local file
         * system, HDFS, Cassandra, HBase, Amazon S3, etc. Spark supports text files, SequenceFiles, and any other
         * Hadoop InputFormat.
         */
        JavaRDD< String > inputRDD = sc.textFile( "src/main/resources/us-500.csv", 3 );
        // Passing functions to spark
        // map
        JavaRDD< String[ ] > splitRDD = inputRDD.map( row -> row.split( "," ) );
        // map to pair
        JavaPairRDD< String, Integer > stateToOneRDD = splitRDD
                .mapToPair( split -> new Tuple2< String, Integer >( split[ 6 ], 1 ) );
        // reduce by Key transformation
        JavaPairRDD< String, Integer > countsRDD = stateToOneRDD.reduceByKey( ( Integer a, Integer b ) -> a + b );
        // collect as map
        Map< String, Integer > collectAsMap = countsRDD.collectAsMap( );
        collectAsMap.forEach( ( String state, Integer count ) -> System.out.println( state + "----" + count ) );
    }

    /**
     * RDD by parallelizing the existing collection
     * 
     * @param sc
     */
    private static void rddFromCollection( JavaSparkContext sc ) {
        List< Integer > intList = Arrays.asList( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 );
        JavaRDD< Integer > intRDD = sc.parallelize( intList );
        JavaRDD< Integer > evenNumRDD = intRDD.filter( i -> i % 2 == 0 );
        evenNumRDD.reduce( ( Integer b, Integer c ) -> b + c );
        evenNumRDD.collect( );
    }
}
