package cs523.FinalProject2;

import java.io.File;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;

public class SparkSQLOnHbase {
	public static void main(String[] args) {

//       
        SparkConf sconf = new SparkConf().setAppName("CSVKafkaStreamReceiver2").setMaster("local[3]");
		sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sconf.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});
        JavaSparkContext jsc = new JavaSparkContext(sconf);
        
        
    	HbaseTableUtil hbaseUtil  = new HbaseTableUtil(jsc, "local[*]");

    	JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = hbaseUtil.readTableByJavaPairRDD();
		System.out.println("Number of register in hbase table: " + hBaseRDD.count());
    	

    	SQLContext sqlContext = new SQLContext(jsc.sc());
    	JavaRDD<MercedesCars> rows = hBaseRDD.map(x -> {
    		MercedesCars car = new MercedesCars();
    		car.setModel(Bytes.toString(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("model"))));
    		car.setYear(Bytes.toString(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("year"))));
    		car.setPrice(Bytes.toFloat(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("price"))));
    		car.setTransmission(Bytes.toString(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("transmission"))));
    		car.setMilleage(Bytes.toDouble(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("milleage"))));
    		car.setFuelType(Bytes.toString(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("fuelType"))));
    		car.setTax(Bytes.toDouble(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("tax"))));
    		car.setMpg(Bytes.toFloat(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("mpg"))));
    		car.setEngineSize(Bytes.toFloat(x._2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("engineSize"))));
    		return car;
		});
    	

    	
    	DataFrame tabledata = sqlContext.createDataFrame(rows, MercedesCars.class);
    	tabledata.registerTempTable("mercedesDB");
    	tabledata.printSchema();
    	
    	// Query 1
    	DataFrame query1 = sqlContext.sql("SELECT * FROM mercedesDB limit 5");
    	query1.show();
    	

    	// Query 2
    	DataFrame query2 = sqlContext.sql("SELECT sum(volume), index FROM mercedesDB group by index order by sum(volume) desc");
    	query2.show();
    	

//    	// Query 3
//    	DataFrame query3 = sqlContext.sql("SELECT max(closeUSD), index FROM stocktrades group by index order by max(closeUSD) desc");
//    	query3.show();
    	
    	
    	jsc.stop();
    	
	}
}
