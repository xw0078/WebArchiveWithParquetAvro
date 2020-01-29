/************************************************
  ArchiveSpark (AS, WARC-CDX experiments) Version: 2.7.6
  ArchiveUnleashedToolkit (AUT, WARC experiments) Version: 0.17
************************************************/


/************************************************
  Parquet/Avro File Writing and Configurations
************************************************/
sqlContext.setConf("parquet.dictionary.page.size", "5242880")
sqlContext.setConf("parquet.block.size", "33554432")
sqlContext.setConf("parquet.filter.statistics.enabled", "true") // ensures predicate pushdown
sqlContext.setConf("parquet.filter.dictionary.enabled", "true") // ensures predicate pushdown
sqlContext.setConf("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") // ensures predicate pushdown 
sqlContext.setConf("spark.sql.parquet.enableVectorizedReader", "false")
// Parquet
data.sort("timestamp").coalesce(1).write.format("parquet")
    .option("compression","gzip").mode("overwrite").save("destination_path")
// Avro
data.sort("timestamp").coalesce(1).write.format("avro")
    .option("compression","gzip").mode("overwrite").save("destination_path")
    
/************************************************
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
// AU: AU only provides for year,month,day filtering
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


