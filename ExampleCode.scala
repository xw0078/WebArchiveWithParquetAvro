/************************************************
  ArchiveSpark (AS, WARC-CDX experiments) Version: 2.7.6
  ArchiveUnleashedToolkit (AUT, WARC experiments) Version: 0.17
************************************************/

/************************************************
  Dependencies
************************************************/

import java.lang.System.nanoTime
import scala.math.pow
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try
import org.apache.spark.sql.types.{StringType, StructField, StructType,IntegerType,LongType}
import java.io.{FileOutputStream, PrintStream}
import java.lang.System.nanoTime
import java.time.LocalDateTime
import org.apache.spark.sql.Row
import scala.io.Source
import scala.math.pow
import scala.util.control.Breaks._
import java.lang.System.nanoTime
import scala.math.pow
import org.apache.spark.sql.DataFrameWriter

import java.util.regex.Pattern
import io.archivesunleashed._
import io.archivesunleashed.matchbox._

/************************************************
  Parquet/Avro File Generation and Configurations
************************************************/

// Generate Parquet file from WARC with customized AUT
val AUT_conversion_schema = StructType( // master schema for AU convertion
      List(
        StructField(name = "key", dataType = StringType, nullable = false),
        StructField(name = "surtUrl", dataType = StringType, nullable = false),
        StructField(name = "timestamp", dataType = StringType, nullable = false), 
        StructField(name = "originalUrl", dataType = StringType, nullable = false),
        StructField(name = "mime", dataType = StringType, nullable = false),
        StructField(name = "status", dataType = StringType, nullable = false),
        StructField(name = "digest", dataType = StringType, nullable = false),
        StructField(name = "redirectUrl", dataType = StringType, nullable = false),
        StructField(name = "meta", dataType = StringType, nullable = false),
        StructField(name = "contentLength", dataType = LongType, nullable = false),
        StructField(name = "offset", dataType = LongType, nullable = false),
        StructField(name = "filename", dataType = StringType, nullable = false),
        StructField(name = "allheader", dataType = StringType, nullable = false),
        StructField(name = "payload", dataType = StringType, nullable = false)
    )
)

// parquet writing conf
sqlContext.setConf("parquet.dictionary.page.size", "5242880")
sqlContext.setConf("parquet.block.size", "33554432")
sqlContext.setConf("parquet.filter.statistics.enabled", "true") 
sqlContext.setConf("parquet.filter.dictionary.enabled", "true") 
sqlContext.setConf("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") // ensures correct timestamp format with predicate pushdown
sqlContext.setConf("spark.sql.parquet.enableVectorizedReader", "false")

// use AUT to convert WARC to Parquet/Avro
def NulltoString(n: Any): String = {
  if (n == null) {
    "-"
  } else {
    n.toString
  }
}

val warc_rdd = { RecordLoader.loadArchives(warcPath, sc)
            .map(r => Row((r.getSurt+r.getOffset.toString+outFileName),r.getSurt,r.getCrawlDate,r.getUrl,r.getMimeType,r.getHttpStatus,NulltoString(r.getDigest),r.getRedirect,r.getMeta,r.getLength,r.getOffset,outFileName,NulltoString(r.getHeaderFields),NulltoString(r.getContentString)))
            }
val warc_df = sqlContext.createDataFrame(all_rdd,AUT_conversion_schema)
warc_df.coalesce(1).write.format("parquet").option("compression","gzip").mode("overwrite").save(outPath)


// note that AUT "r.getCrawlDate" function does not extract exact unix timestamp from the WARC, 
// for our experiments, we decide to extract the accurate timestamp from the WARC header as an addtion step so that we can get the timestamp type column.
// (this step can be optimized through modifying AUT to reduce multiple conversions)
val fixTimestamp = (s:String) => {
    if (s != null){
        //print(s.getClass.toString)
        val Pattern = "(2018-05-.*)".r
        Pattern.findFirstIn(s).getOrElse("null").replace("T"," ").replace("Z","")
    }
    else{
        "null"
    }
}
val fixTimestampUDF = udf(fixTimestamp)
val parquetDf =  spark.read.format("parquet").schema(AU_conversion_schema).load("source path")
val fixedDf = parquetDf.withColumn("timestamp",fixTimestampUDF(col("allheader"))).withColumn("timestamp",unix_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"))
fixedDf.coalesce(1).write.format("parquet").option("compression","gzip").mode("overwrite").save("destination path")

// Avro Conversion instead of Parquet
data.coalesce(1).write.format("avro")
    .option("compression","gzip").mode("overwrite").save("destination_path")
    
/************************************************
  Workload Part
  Data loading
************************************************/    
// Parquet / Avro
val df = spark.read.format("parquet").load("data_dir/*.parquet")
val df = spark.read.format("avro").load("data_dir/*.avro")
// AS
val records = ArchiveSpark.load(WarcCdxHdfsSpec(cdxPath,warcPath))
// AUT
val records = RecordLoader.loadArchives(warcPath, sc)

/************************************************
  Data Count
************************************************/  
// Parquet,Avro
val count = df.count
// AS / AUT 
val count = records.count

/************************************************
  Timestamp Filtering
************************************************/  
// Parquet,Avro
val filtered = df.filter(col("timestamp") > unix_timestamp(lit(timeFrom)).cast("timestamp") && col("timestamp") < unix_timestamp(lit(timeTo)).cast("timestamp")) // INT64 timestamp with predicate pushdown
val filtered = df.withColumn("timestampStr",unix_timestamp($"timestampStr", "yyyy-MM-dd HH:mm:ss").cast("timestamp")).filter(col("timestampStr") > unix_timestamp(lit(timeFrom)).cast("timestamp") && col("timestampStr") < unix_timestamp(lit(timeTo)).cast("timestamp")) // string timestamp without predicate pushdown
// AS
val filtered = records.filter(r => r.time.isAfter(timeFrom) && r.time.isBefore(timeTo)).enrich(StringContent)
// AUT: AUT only provides for year,month,day filtering
val filtered = RecordLoader.loadArchives(warcPath, sc).keepDate(List(time),ExtractDate.DateComponent.YYYYMMDD)

/************************************************
  URL Filtering
************************************************/  
// Parquet,Avro
val filtered = df.filter($"originalUrl".isin(URLs:_*)) 
// Parquet (D-PP)
val filtered = df.filter($"domain".isin(domains:_*)).filter($"originalUrl".isin(URLs:_*)) 
// AS
val filtered = records.filter(r => URLs.contains(r.originalUrl)).enrich(StringContent) 
// AU
val filtered = RecordLoader.loadArchives(warcPath, sc).keepUrlPatterns(URLs.toSet)

/************************************************
  Topic Modeling
  Reference Link: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3741049972324885/3783546674231782/4413065072037724/latest.html
************************************************/  
// extract puretext
import org.jsoup.Jsoup
val html_extractor = (strpayload:String) => {
    Jsoup.parse(strpayload).text().replaceAll("[\\r\\n]+", " ")
}
val text_extractorUDF = udf(html_extractor)
val puretext = df.withColumn("puretext",text_extractorUDF($"payload")).select("puretext") // Parquet,Avro
val puretext = records.enrich(HtmlText).map(r => r.valueOrElse(HtmlText,"")).toDF("puretext") // AS
val puretext = records.map(r => (RemoveHTML(r.getContentString))).toDF("puretext") // AU

// Apply LDA pipeline to extracted puretext
val indexedDf = puretext.withColumn("id",monotonicallyIncreasingId)
val tokenizer = new RegexTokenizer().setPattern("[\\W_]+").setMinTokenLength(4).setInputCol("puretext").setOutputCol("tokens")
val tokenized_df = tokenizer.transform(indexedDf)
val stopwords = sc.textFile("/path/to/stopwords").collect()
val remover = new StopWordsRemover().setStopWords(stopwords).setInputCol("tokens").setOutputCol("filtered")
val filtered_df = remover.transform(tokenized_df)
val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
val countVectors = vectorizer.transform(filtered_df).select("id", "features")
val lda_countVector = countVectors.map { case Row(id: Long, countVector: org.apache.spark.ml.linalg.Vector) => (id, Vectors.fromML(countVector)) }.rdd
val numTopics = 20
val lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8)).setK(numTopics).setMaxIterations(3)
    .setDocConcentration(-1).setTopicConcentration(-1)
lda.run(lda_countVector)


