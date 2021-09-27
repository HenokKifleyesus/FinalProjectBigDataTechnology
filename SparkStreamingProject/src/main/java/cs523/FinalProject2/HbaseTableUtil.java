package cs523.FinalProject2;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import com.google.gson.Gson;

import scala.Tuple2;



public class HbaseTableUtil 
{

	private static final String TABLE_NAME = "mercedesDB";
	private static final String CF_DEFAULT = "cf";
	static Configuration config;
	JavaSparkContext jsc;
	String mode;
	Job newAPIJobConfiguration;
	
	public HbaseTableUtil (JavaSparkContext jsc, String mode) {
		this.jsc = jsc;
		this.mode = mode;
		config = HBaseConfiguration.create();
		config.addResource(new Path("file:///etc/hbase/conf.dist/hbase-site.xml"));
		config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
		
		// new Hadoop API configuration
		
		try {
			newAPIJobConfiguration = Job.getInstance(config);
			newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
			newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			
			// old Hadoop API configuration
//			JobConf oldAPIJobConfiguration = new JobConf(config, SparkToHBase.class);
//			oldAPIJobConfiguration.setOutputFormat(TableOutputFormat.class);
//			oldAPIJobConfiguration.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
			
			this.initialize(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    public void initialize(Configuration config)
    {
        System.out.println( "init spark hbase!" );

    	HBaseAdmin hBaseAdmin = null;
        try  {
        	hBaseAdmin = new HBaseAdmin(config);

			System.out.print("Creating table.... ");
        	HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			
			if (hBaseAdmin.tableExists(table.getTableName()))
			{
				System.out.print("table already created.... ");
//				hBaseAdmin.disableTable(table.getTableName());
//				hBaseAdmin.deleteTable(table.getTableName());
			}else 
				hBaseAdmin.createTable(table);

			System.out.println(" Done!");
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    void writeRowNewHadoopAPI(JavaPairRDD<String, String> records) {
    	JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = 
    			records.mapToPair(x -> {
					Gson gson = new Gson();
					MercedesCars car = gson.fromJson(x._2, MercedesCars.class);
    				Put put = new Put(Bytes.toBytes("rowkey." + car.getModel() + "." + car.getPrice() + "." + car.getMilleage()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("model"), Bytes.toBytes(car.getModel()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("year"), Bytes.toBytes(car.getYear()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("price"), Bytes.toBytes(car.getPrice()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("transmission"), Bytes.toBytes(car.getTransmission()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("milleage"), Bytes.toBytes(car.getMilleage()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("fuelType"), Bytes.toBytes(car.getFuelType()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("tax"), Bytes.toBytes(car.getTax()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("mpg"), Bytes.toBytes(car.getMpg()));
					put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("engineSize"), Bytes.toBytes(car.getEngineSize()));
					
    				return new Tuple2<ImmutableBytesWritable, Put>(
						new ImmutableBytesWritable(), put);});
 		hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
    }
    
    RDD<Tuple2<ImmutableBytesWritable, Result>> readTable() {
		RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = jsc.sc()
				.newAPIHadoopRDD(
						config,
						TableInputFormat.class,
						org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
						org.apache.hadoop.hbase.client.Result.class);
		return hBaseRDD;
	}
    
    JavaPairRDD<ImmutableBytesWritable, Result> readTableByJavaPairRDD() {
		
    	JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc
				.newAPIHadoopRDD(
						config,
						TableInputFormat.class,
						org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
						org.apache.hadoop.hbase.client.Result.class);
		return hBaseRDD;
    }
	
}
