import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.hive.HiveContext
import java.util.Calendar
import org.apache.phoenix.spark._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.hive.HiveContext
import java.util.Calendar
import org.apache.phoenix.spark._

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import scala.util.Random
import org.apache.spark.sql.functions._
//
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}
//
import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
//
import org.apache.log4j.Logger
import org.apache.log4j.Level

//
object md_HbaseMongo
{
  private var zookeeperUrl = "rhes75:2181"
  private var requestConsumerId = null
  private var impressionConsumerId = null
  private var clickConsumerId = null
  private var conversionConsumerId = null
  private var requestTopicName = null
  private var impressionTopicName = null
  private var clickTopicName = null
  private var conversionTopicName = null
  private var requestThreads = 0
  private var impressionThreads = 0
  private var clickThreads = 0
  private var conversionThreads = 0
  private var sparkAppName = "md_HbaseMongo"
  private var sparkMasterUrl = "local[12]"
  private var sparkDefaultParllelism = null
  private var sparkDefaultParallelismValue = "12"
  private var sparkSerializer = null
  private var sparkSerializerValue = "org.apache.spark.serializer.KryoSerializer"
  private var sparkNetworkTimeOut = null
  private var sparkNetworkTimeOutValue = "3600"
  private var sparkStreamingUiRetainedBatches = null
  private var sparkStreamingUiRetainedBatchesValue = "5"
  private var sparkWorkerUiRetainedDrivers = null
  private var sparkWorkerUiRetainedDriversValue = "5"
  private var sparkWorkerUiRetainedExecutors = null
  private var sparkWorkerUiRetainedExecutorsValue = "30"
  private var sparkWorkerUiRetainedStages = null
  private var sparkWorkerUiRetainedStagesValue = "100"
  private var sparkUiRetainedJobs = null
  private var sparkUiRetainedJobsValue = "100"
  private var sparkJavaStreamingDurationsInSeconds = "10"
  private var sparkNumberOfSlaves = 14
  private var sparkRequestTopicShortName = null
  private var sparkImpressionTopicShortName = null
  private var sparkClickTopicShortName = null
  private var sparkConversionTopicShortName = null
  private var sparkNumberOfPartitions = 30
  private var sparkClusterDbIp = null
  private var clusterDbPort = null
  private var insertQuery = null
  private var insertOnDuplicateQuery = null
  private var sqlDriverName = null
        //  private var configFileReader: ConfigFileReader = null

 private var dbConnection = "mongodb"
  private var dbDatabase = "trading"
  private var dbPassword = "mongodb"
  private var dbUsername = "trading_user_RW"
  private var bootstrapServers = "rhes75:9092, rhes75:9093, rhes75:9094, rhes564:9092, rhes564:9093, rhes564:9094, rhes76:9092, rhes76:9093, rhes76:9094"
  private var schemaRegistryURL = "http://rhes75:8081"
  private var zookeeperConnect = "rhes75:2181, rhes564:2181, rhes76:2181"
  private var zookeeperConnectionTimeoutMs = "10000"
  private var rebalanceBackoffMS = "15000"
  private var zookeeperSessionTimeOutMs = "15000"
  private var autoCommitIntervalMS = "12000"
  private var topicsValue = "md"
  private var memorySet = "F"
  private var enableHiveSupport = null
  private var enableHiveSupportValue = "true"
  private var sparkStreamingReceiverMaxRateValue = "0"
  private var checkpointdir = "/checkpoint"
  private var hbaseHost = "rhes75"
  private var mongodbHost = "rhes75"
  private var mongodbPort = "60100"
  private var zookeeperHost = "rhes75"
  private var zooKeeperClientPort = "2181"
  private var batchInterval = 2
  private var tickerWatch = "VOD"
  private var priceWatch: Double = 300.0
  private var op_type = 1
  private var currency = "GBP"
  private var tickerType = "short"
  private var tickerClass = "asset"
  private var tickerStatus = "valid"
  private var confidenceLevel = 1.645
  private var hvTicker = 0

  def main(args: Array[String])
  {
    // Create a StreamingContext with two working thread and batch interval of 2 seconds.

   var startTimeQuery = System.currentTimeMillis
   //var startTime: Long = System.nanoTime()
   //var endTime: Long =  System.nanoTime()

    // Start Hbase table stuff
    val hbaseTableName = "MARKETDATAHBASEBATCH"
    val conf = HBaseConfiguration.create()
//  Connecting to remote Hbase
    conf.set("hbase.master", hbaseHost)
    conf.set("hbase.zookeeper.quorum",zookeeperHost)
    conf.set("hbase.zookeeper.property.clientPort",zooKeeperClientPort)
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)

    // Start MongoDB collection stuff
    val collectionName = "MARKETDATAMONGODBSPEED"
    val connectionString = dbConnection+"://"+dbUsername+":"+dbPassword+"@"+mongodbHost+":"+mongodbPort+"/"+dbDatabase+"."+collectionName

val sparkConf = new SparkConf().
               setAppName(sparkAppName).
               set("spark.driver.allowMultipleContexts", "true").
               set("spark.hadoop.validateOutputSpecs", "false")

             // change the values accordingly.
             sparkConf.set("sparkDefaultParllelism", sparkDefaultParallelismValue)
             sparkConf.set("sparkSerializer", sparkSerializerValue)
             sparkConf.set("sparkNetworkTimeOut", sparkNetworkTimeOutValue)


             // change the values accordingly.
             sparkConf.set("sparkDefaultParllelism", sparkDefaultParallelismValue)
             sparkConf.set("sparkSerializer", sparkSerializerValue)
             sparkConf.set("sparkNetworkTimeOut", sparkNetworkTimeOutValue)
             sparkConf.set("sparkStreamingUiRetainedBatches",
                           sparkStreamingUiRetainedBatchesValue)
             sparkConf.set("sparkWorkerUiRetainedDrivers",
                           sparkWorkerUiRetainedDriversValue)
             sparkConf.set("sparkWorkerUiRetainedExecutors",
                           sparkWorkerUiRetainedExecutorsValue)
             sparkConf.set("sparkWorkerUiRetainedStages",
                           sparkWorkerUiRetainedStagesValue)
             sparkConf.set("sparkUiRetainedJobs", sparkUiRetainedJobsValue)
             sparkConf.set("enableHiveSupport",enableHiveSupportValue)
             if (memorySet == "T")
             {
               sparkConf.set("spark.driver.memory", "18432M")
             }
             sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
             sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
             sparkConf.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
             sparkConf.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
             sparkConf.set("spark.streaming.backpressure.enabled","true")
             sparkConf.set("spark.streaming.receiver.maxRate",sparkStreamingReceiverMaxRateValue)
             sparkConf.set("spark.mongodb.input.uri", connectionString)
             sparkConf.set("spark.mongodb.output.uri", connectionString)


    val streamingContext = new StreamingContext(sparkConf, Seconds(batchInterval))
    val sparkContext  = streamingContext.sparkContext
    val sqlContext= new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._
    sparkContext.setLogLevel("ERROR")

       // Back to MongoDB
    val rdd = MongoSpark.load(sparkContext)
    val MARKETDATAMONGODBSPEED = rdd.toDF
    MARKETDATAMONGODBSPEED.printSchema


    sparkContext.setLogLevel("ERROR")

    var sqltext = ""
    var totalPrices: Long = 0
    val runTime = 240
    var stats = new Array[Double](10)

   // Load data from Hbase table
   val HbaseFunctions = new HbaseFunctions
 
  val dfHbase = HbaseFunctions.withCatalog(sqlContext, HbaseFunctions.catalog)
  dfHbase.printSchema
    val HbaseAverages = new HbaseAverages
    var rows = 0
    // get No of Docs in MongoDB collections
    rows = MARKETDATAMONGODBSPEED.count.toInt
    println("Documents in " + collectionName + ": " + rows)

    val writeConfig = WriteConfig(Map("collection" -> collectionName, "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
    
    val kafkaParams = Map[String, String](
                                      "bootstrap.servers" -> bootstrapServers,
                                      "schema.registry.url" -> schemaRegistryURL,
                                       "zookeeper.connect" -> zookeeperConnect,
                                       "group.id" -> sparkAppName,
                                       "zookeeper.connection.timeout.ms" -> zookeeperConnectionTimeoutMs,
                                       "rebalance.backoff.ms" -> rebalanceBackoffMS,
                                       "zookeeper.session.timeout.ms" -> zookeeperSessionTimeOutMs,
                                       "auto.commit.interval.ms" -> autoCommitIntervalMS
                                     )
    //val topicsSet = topics.split(",").toSet
    val topics = Set(topicsValue)
    val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    dstream.cache()
    val comma = ", "
    val q = """""""
    var current_timestamp = "current_timestamp"
    // Work on every Stream
    dstream.foreachRDD
    { pricesRDD =>
      if (!pricesRDD.isEmpty)  // data exists in RDD
      {

        val op_time = System.currentTimeMillis.toString
        val spark = SparkSessionSingleton.getInstance(pricesRDD.sparkContext.getConf)
        val sc = spark.sparkContext
        import spark.implicits._
        var operation = new operationStruct(op_type, op_time)

        // Convert RDD[String] to RDD[case class] to DataFrame
        val RDDString = pricesRDD.map { case (_, value) => value.split(',') }.map(p => priceDocument(priceStruct(p(0).toString,p(1).toString,p(2).toString,p(3).toDouble, currency), operation))
        val df = spark.createDataFrame(RDDString).cache
         // loop through individual rows and get values. Work out upper & lower

         // Work on individual messages
         for(row <- pricesRDD.collect.toArray)
         {
           var ticker =  row._2.split(',').view(1).toString
           var price = row._2.split(',').view(3).toFloat
	   val dfShort = dfHbase.filter(col("ticker") === ticker).sort(col("timeissued").desc).limit(14).cache
           var stats =  HbaseAverages.tickerStats(dfShort)
           var Average = stats(0)
           var standardDeviation = stats(1)
           var lower = Average - confidenceLevel * standardDeviation
           var upper = Average + confidenceLevel * standardDeviation
           hvTicker = HbaseAverages.priceComparison(ticker, price, lower, upper)
           if(hvTicker == 1) {
             var document = df.filter('priceInfo.getItem("ticker").contains(ticker))
             MongoSpark.save(document, writeConfig)
             //totalPrices += document.count
             //println("Current time is: " + Calendar.getInstance.getTime)
             var endTimeQuery = System.currentTimeMillis
             // Check if running time > runTime exit
             if( (endTimeQuery - startTimeQuery)/(100000*60) > runTime)
             {
               println("\nDuration exceeded " + runTime + " minutes exiting")
               System.exit(0)
             }
             //println("Total high value prices added to the collection so far: " +totalPrices+ " , Runnig for  " + (endTimeQuery - startTimeQuery)/(1000*60)+" Minutes")
           }
         }
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination() 
    //streamingContext.stop()
  }
}

case class operationStruct (op_type: Int, op_time: String)
case class tradeStruct (tickerType: String, tickerClass: String, tickerStatus: String, tickerQuotes: Array[Double])
case class priceStruct(key: String, ticker: String, timeissued: String, price: Double, currency: String)
case class priceDocument(priceInfo: priceStruct, operation: operationStruct)

class UsedFunctions {
  import scala.util.Random
  import scala.math._
  import org.apache.spark.sql.functions._
  def randomString(chars: String, length: Int): String =
     (0 until length).map(_ => chars(Random.nextInt(chars.length))).mkString
  def clustered(id : Int, numRows: Int) : Double  = (id - 1).floor/numRows
  def scattered(id : Int, numRows: Int) : Double  = (id - 1 % numRows).abs
  def randomised(seed: Int, numRows: Int) : Double  = {
  var end = numRows
   if (end == 0) end = Random.nextInt(1000)
     return (Random.nextInt(seed) % end).abs
  }
  def padString(id: Int, chars: String, length: Int): String =
     (0 until length).map(_ => chars(Random.nextInt(chars.length))).mkString + id.toString
  def padSingleChar(chars: String, length: Int): String =
     (0 until length).map(_ => chars(Random.nextInt(chars.length))).mkString
}

class HbaseFunctions {
  // Define Hbase table catalog here. Note cf is case sensitive
  def catalog = s"""{
    |"table":{"namespace":"default", "name":"MARKETDATAHBASEBATCH"},
    |"rowkey":"key",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
    |"ticker":{"cf":"PRICE_INFO", "col":"ticker", "type":"string"},
    |"timeissued":{"cf":"PRICE_INFO", "col":"timeissued", "type":"string"},
    |"price":{"cf":"PRICE_INFO", "col":"price", "type":"string"}
    |}
|}""".stripMargin

  // Define method to get DF around table MARKETDATAHBASEBATCH
  def withCatalog(sqlContext: org.apache.spark.sql.SQLContext, cat: String): DataFrame = {
    sqlContext
    .read
    .options(Map(HBaseTableCatalog.tableCatalog->cat))
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()
 }
}
class HbaseAverages {
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.sql.functions._
  import java.util.{Currency, Locale}
  import java.util.Locale
  val gbp = Currency.getInstance(new Locale("gb", "GB"))
  val formatter = java.text.NumberFormat.getCurrencyInstance
  formatter.setCurrency(gbp)

  def tickerStats(dfShort: org.apache.spark.sql.DataFrame): Array[Double] = {
    var arr = new Array[Double](10)
    var priceTicker = dfShort.
                                   select(avg(col("price")),stddev(col("price"))).
                                   collect.apply(0)
    arr = Array(priceTicker.getDouble(0),priceTicker.getDouble(1))
    return arr
  }
  def priceComparison (ticker: String, price: Double, lower: Double, upper: Double): Int = {
   var hvTicker = 0    
   if(price >= upper) {
      println("\n*** price for ticker " + ticker + " is " + formatter.format(price) + " >= " + formatter.format(upper) + " SELL ***")
      //println("\n*** price for ticker " + ticker + " is " + f"$price%06.2f" + " >= " + f"$upper%06.2f" + " SELL ***")
      hvTicker = 1
    }
    if(price <= lower) {
      println("\n*** price for ticker " + ticker + " is " + formatter.format(price) + " <= " + formatter.format(upper) + " BUY ***")
      //println("\n*** price for ticker " + ticker + " is " + f"$price%06.2f" + " <= " + f"$lower%06.2f" + " BUY ***")
      hvTicker = 1
    }
    return hvTicker  
  }
}

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
