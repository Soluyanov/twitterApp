package com.test.core.streaming

import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.log4j.Logger
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream._
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.io.{
  ArrayWritable,
  BooleanWritable,
  BytesWritable,
  DoubleWritable,
  FloatWritable,
  IntWritable,
  LongWritable,
  NullWritable,
  Text,
  Writable
}
import org.apache.spark.rdd.PairRDDFunctions
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.io.compress.GzipCodec
import java.io.PrintWriter
import org.apache.hadoop.fs.FSDataOutputStream
import java.io._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.io.Source
import org.apache.spark.sql.functions.unix_timestamp
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.streaming.twitter._
import twitter4j._
import twitter4j.auth.AuthorizationFactory
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaPairDStream
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext

/**
  * Application used to write rsyslog messages to hdfs
  */
object Fake {

  val Log = Logger.getLogger(Fake.this.getClass().getSimpleName())
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        "Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
          "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, source, pathToStore) = args
    val sparkConf = new SparkConf().setAppName("Fake")
    val sc = new SparkContext(sparkConf)

     sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(60))


  
    /**
      * Writes messages in files and specifies the behavior, when target
      * file exists or not
      */

      /**
  val config = new   twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey("7GWZiG3xSSVXUpoXhLAdTwcr4")
    .setOAuthConsumerSecret("Cqmm3WFwP4Z8sV0payW9CsYnKOWZYzhr1NrNYD49PTeVrd5f0L")
    .setOAuthAccessToken("376173910-Vms6JpjWjdXcYbMQrlXQ5hhN7QSlUrK17rGgrdlb")
    .setOAuthAccessTokenSecret("pMqqXGpLhBMyyesCbxQtyaGLYs3ysoH2RuuZf7m6yHUFA")
    .build


    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status) { println(status.getText) }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex: Exception) { ex.printStackTrace }
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    }

    val twitterStream = new TwitterStreamFactory(config).getInstance
    twitterStream.addListener(simpleStatusListener)
    twitterStream.filter(new FilterQuery(Array(376173910)))
    Thread.sleep(10000)
    twitterStream.cleanUp
    twitterStream.shutdown
*/


    System.setProperty("twitter4j.oauth.consumerKey", "7GWZiG3xSSVXUpoXhLAdTwcr4")
System.setProperty("twitter4j.oauth.consumerSecret", "Cqmm3WFwP4Z8sV0payW9CsYnKOWZYzhr1NrNYD49PTeVrd5f0L")
System.setProperty("twitter4j.oauth.accessToken", "376173910-Vms6JpjWjdXcYbMQrlXQ5hhN7QSlUrK17rGgrdlb")
System.setProperty("twitter4j.oauth.accessTokenSecret", "pMqqXGpLhBMyyesCbxQtyaGLYs3ysoH2RuuZf7m6yHUFA")

    val stream = TwitterUtils.createStream(ssc, None)


    val tweets = stream.filter {t =>
      val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      tags.contains("#bigdata") && tags.contains("#oscar")
    }

             tweets.saveAsTextFiles("/home/alexander/Downloads/tweet")


    Log.error("DEBUG info:" + zkQuorum)
    sys.ShutdownHookThread({
      println("Ctrl+C")
      try {
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      } catch {
        case e: Throwable => {
          println("exception on ssc.stop(true, true) occured")
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()



  }
}
