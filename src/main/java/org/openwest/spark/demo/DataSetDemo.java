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
        // DataFrame is a collection of distributed data organized into named columns called DataFrame
        Dataset< Row > csvDataFrame = sparkSession.read( ).format( "csv" ).option( "header", "true" )
                .csv( "src/main/resources/us-500.csv" )
                .withColumn( "zip", new Column( "zip" ).cast( DataTypes.IntegerType ) );
        csvDataFrame.createOrReplaceTempView( "people" );
        Dataset< Row > sqlResult = sparkSession.sql( "select * from people where state = 'CA'" );
        sqlResult.show( );
        sqlResult.where( "city = 'San Jose'" ).show( );
        /**
         * Dataset is a strongly-typed, immutable collection of objects that are mapped to a relational schema. It is
         * conceptually
         * equivalent to a table in a relational database or a R/Python Dataframe.
         */
        Dataset< Person > peopleDataset = csvDataFrame.as( Encoders.bean( Person.class ) );
        peopleDataset.printSchema( );
        Dataset< Person > peopleList = peopleDataset.filter( p -> p.getState( ).equals( "CA" ) );
        peopleList.foreach( p -> System.out.println( p.getFirstName( ) + "  " + p.getLastName( ) ) );
    }
}
