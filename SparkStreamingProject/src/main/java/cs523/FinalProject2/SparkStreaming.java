package cs523.FinalProject2;

import java.io.IOException;
import java.util.*;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
//import org.apache.spark.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;

import com.google.gson.Gson;

import scala.Tuple2;


public class SparkStreaming 
{
	static Configuration config;
	static Job newAPIJobConfiguration;
	
    @SuppressWarnings("deprecation")
	public static void main( String[] args )
    {
        System.out.println( "starting spark steaming!" );
        SparkConf sconf = new SparkConf().setAppName("KafkaFinalProject2").setMaster("local[3]");
		sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sconf.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});
        JavaSparkContext jsc = new JavaSparkContext(sconf);
    	JavaStreamingContext ssc = new JavaStreamingContext(jsc, new Duration(1000));
    	
		config = HBaseConfiguration.create();
		config.addResource(new Path("file:///etc/hbase/conf.dist/hbase-site.xml"));
		config.set(TableInputFormat.INPUT_TABLE, "mercedesDB");
    	HBaseAdmin hBaseAdmin = null;
        try  {

			newAPIJobConfiguration = Job.getInstance(config);
			newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "mercedesDB");
			newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			
        	hBaseAdmin = new HBaseAdmin(config);

			System.out.print("Creating table.... ");
        	HTableDescriptor table = new HTableDescriptor(TableName.valueOf("mercedesDB"));
			table.addFamily(new HColumnDescriptor("cf").setCompressionType(Algorithm.NONE));
			
			if (hBaseAdmin.tableExists(table.getTableName()))
			{
				System.out.print("table already created.... ");
			}else {
				hBaseAdmin.createTable(table);
			}


//    		RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD= hbaseUtil.readTable();
//    		System.out.println("Number of register in hbase table: " + hBaseRDD.count());
    		
    	    Set<String> topicsSet = new HashSet<>(Arrays.asList("mercedes".split(",")));
    		Map<String, String> kafkaParams = new HashMap<>();
    	    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    	    
    		JavaPairInputDStream<String, String> directKafkaStream = 
    			     KafkaUtils.createDirectStream(ssc,
    			    	        String.class, String.class, StringDecoder.class, StringDecoder.class, 
    			    	        kafkaParams, topicsSet);
    		

    		directKafkaStream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
				@Override
				public Void call(JavaPairRDD<String, String> rdd) throws Exception {
					// TODO Auto-generated method stub
					System.out.println( "rds!" + rdd.count() );
    	    		writeRowNewHadoopAPI(newAPIJobConfiguration.getConfiguration(), rdd);
    	    		return null;
				}
    		});
    		ssc.start();
    		ssc.awaitTermination();
    		jsc.sc().stop();
    		
//	    	
	    	
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println( "complete spark stream!" );
    }
    
    static void writeRowNewHadoopAPI(Configuration config, JavaPairRDD<String, String> records) {
    	JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = 
    			records.mapToPair(x -> {
					Gson gson = new Gson();
					MercedesCars car = gson.fromJson(x._2, MercedesCars.class);
    				Put put = new Put(Bytes.toBytes("rowkey." + car.getModel() + "." + car.getPrice() + "." + car.getMilleage()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("model"), Bytes.toBytes(car.getModel()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("year"), Bytes.toBytes(car.getYear()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("price"), Bytes.toBytes(car.getPrice()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("transmission"), Bytes.toBytes(car.getTransmission()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("milleage"), Bytes.toBytes(car.getMilleage()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fuelType"), Bytes.toBytes(car.getFuelType()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("tax"), Bytes.toBytes(car.getTax()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("mpg"), Bytes.toBytes(car.getMpg()));
					put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("engineSize"), Bytes.toBytes(car.getEngineSize()));
					
    				return new Tuple2<ImmutableBytesWritable, Put>(
						new ImmutableBytesWritable(), put);});
 		hbasePuts.saveAsNewAPIHadoopDataset(config);
    }
}
