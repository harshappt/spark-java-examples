package org.openwest.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.openwest.spark.model.Person;

public class DataSetDemo {
    public static void main( String[ ] args ) {
        SparkConf conf = new SparkConf( ).setAppName( "DSAndDFDemo" ).setMaster( "local[*]" );
        SparkSession sparkSession = new SparkSession( new SparkContext( conf ) );
        Dataset< Row > csvDataFrame = sparkSession.read( ).format( "csv" ).option( "header", "true" )
                .csv( "src/main/resources/us-500.csv" )
                .withColumn( "zip", new Column( "zip" ).cast( DataTypes.IntegerType ) );
        csvDataFrame.show( );
        csvDataFrame.createOrReplaceTempView( "people" );
        Dataset< Row > sqlResult = sparkSession.sql( "select state,zip,count(*) from people group by state,zip" );
        sqlResult.printSchema( );
        Dataset< Person > peopleDataset = csvDataFrame.as( Encoders.bean( Person.class ) );
        // SQLContext sqlContext = new SQLContext( new Ja )
        // peopleDataset.filter( p -> p.getState( ) != null && ).filter( p -> p.getState( ).equalsIgnoreCase( "" ) )
    }
}
