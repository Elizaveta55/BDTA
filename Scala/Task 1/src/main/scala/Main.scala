import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


class Main {


  def read_data(path: String, spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .load(path)
      .select("ItemID","Sentiment","SentimentText")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("My name")
      .getOrCreate()

    val conf = new SparkConf()
      .setAppName("My name")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import spark.implicits._

    val data = read_data("src/main/twitter_sentiment_data.csv", spark)

    val removeRepetitive = udf{ str: String => str.replaceAll("((.))\\1+","$1").trim.toLowerCase()}
    val noRepetitiveData = data.withColumn("Collapsed", removeRepetitive('SentimentText))

    val tokenizer_sentences = new RegexTokenizer()
      .setInputCol("PreprocessedSentences")
      .setOutputCol("Tokens")
      .setPattern("\\.")
    val sentenceData = tokenizer_sentences.transform(noRepetitiveData)

    val tokenizer_words = new RegexTokenizer()
      .setInputCol("PreprocessedWords")
      .setOutputCol("Tokens")
      .setPattern("\\s+")
    val wordsData = tokenizer_words.transform(sentenceData)

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
    val removedWordsData = remover.transform(wordsData)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("tf").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(removedWordsData)

    val idfModel = new IDF().setInputCol("tfFeatures").setOutputCol("tfidfFeatures").fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val lr = new LogisticRegression()
      .setFamily("multinomial")
      .setFeaturesCol("tfidfFeatures")
      .setLabelCol("Sentiment")
    val lrModel = lr.fit(rescaledData)


    val pipe = new Pipeline()
      .setStages(Array(
        tokenizer_sentences,
        tokenizer_words,
        remover,
        hashingTF,
        idfModel,
        lr
      ))


    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.tol, Array(1e-20, 1e-10, 1e-5))
      .addGrid(lr.maxIter, Array(100, 200, 300))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipe)
      .setEvaluator(new BinaryClassificationEvaluator()
        .setRawPredictionCol("prediction")
        .setLabelCol("Sentiment"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice
      .setParallelism(2)

    val modelCV = cv.fit(noRepetitiveData)

    val result = modelCV.transform(noRepetitiveData)
  }


}
