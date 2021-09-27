package cs523.FinalProject;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaSparkContext;

public class KafkaSparkStreaming 
{
	static Boolean keepReading = true;
    public static void main( String[] args )
    {
        System.out.println( "starting file stream reader!" );
        String inputFilePath = args[0];

    	JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("KafkaFinalProject").setMaster("local[3]"));
//    	JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));
    	SparkSession spark = SparkSession.builder().appName("KafkaFinalProject")
    			.getOrCreate();
    	

        try {
	    	// Read all the csv files written atomically in a directory
	    	StructType userSchema = new StructType()
	    	.add("model", "string").add("year", "string")
	    	.add("price", "float").add("transmission", "string").add("milleage", "double")
	    	.add("fuelType", "string").add("tax", "double").add("mpg", "float").add("engineSize", "float");
	    	
	    	Dataset<Row> dfCSV = spark.readStream()
	    			.option("sep", ",").option("header", "false")
	    			.schema(userSchema).csv(inputFilePath);

	    	dfCSV.printSchema();
	    			
	    	StreamingQuery csvQuery = dfCSV
	    			.where(functions.col("model").isNotNull())
//	    			.select("Date").as("key")
	    			.select(functions.to_json(functions.struct(functions.col("*"))).as("value"))
	    			.selectExpr("CAST(value AS STRING)")
	    			.writeStream()
	    			.format("kafka")
	    			.option("kafka.bootstrap.servers", "localhost:9092")  
	    			.option("topic", "mercedes")  
	    			.option("checkpointLocation", "/tmp/cloudera/checkpoint")
	    			.start();
	    	
//	    	StreamingQuery csvQuery2 = dfCSV
//    			.where(functions.col("Index").isNotNull())
//    			.select("Date").as("key")
//    			.select(functions.to_json(functions.struct(functions.col("*"))).as("value"))
//    			.selectExpr("CAST(value AS STRING)")
//    			.writeStream()
//    			.format("console")
//    			.start();
//
    	
    	
	    	csvQuery.awaitTermination();
//	    	csvQuery2.awaitTermination();
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println( "complete file stream reader!" );
    }
}
