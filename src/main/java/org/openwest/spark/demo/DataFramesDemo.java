package org.openwest.spark.demo;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFramesDemo {
    public static void main( String[ ] args ) {
        System.setProperty( "hadoop.home.dir", "C:/Users/hrputhal/Dev/hadoop-winutils-2.6.0" );
        SparkSession sparkSession = SparkSession.builder( ).appName( "DataFrames Demo" ).master( "local[*]" )
                .getOrCreate( );
        Dataset< Row > dataframe = sparkSession.read( ).option( "header", "true" )
                .csv( "C:/Users/hrputhal/Downloads/us-500.csv" );
        dataframe.printSchema( );
        dataframe.createOrReplaceTempView( "people" );
        Dataset< Row > sqlResult = sparkSession.sql( "SELECT state,zip,count(*) from people group by state,zip" );
        sqlResult.collectAsList( ).forEach( row -> System.out
                .println( row.getString( 0 ) + "-----" + row.getString( 1 ) + "-----" + row.getLong( 2 ) + "/n" ) );
        Dataset< Row > count = dataframe.select( new Column( "state" ), new Column( "zip" ) )
                .groupBy( new Column( "state" ), new Column( "zip" ) ).count( );
        count.printSchema( );
        count.collectAsList( ).forEach(
                o -> System.out.println( o.getString( 0 ) + "-----" + o.getString( 1 ) + "------" + o.getLong( 2 ) ) );
    }
}
