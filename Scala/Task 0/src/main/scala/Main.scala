import java.io.File
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors
import java.util.stream.Stream

object MainClass{
  def main(args: Array[String]){

    /*
    val conf = new SparkConf().setAppName("appName")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)

    val textFile = sc.textFile(inputPath)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(outputPath)

     */

    val file: File = new File(
      "C:\\Users\\kolma\\Desktop\\java\\stream\\src\\alice.txt")
    val list: List = Files
      .lines(Paths.get(file.getAbsolutePath))
      .flatMap((s) => Stream.of(s.split(" ")))
      .map((word) => (word))
      .collect(Collectors.toList())
    println(list)
  }
}