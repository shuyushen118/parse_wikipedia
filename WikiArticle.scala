import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WikiArticle {

  val regex_stub = "-stub}}".r
  val regex_redirect = "#redirect".r
  val regex_Disam = "disambiguation}}".r

  def isArticle(v:String): Boolean = {

    if (regex_stub.findAllIn(v).length >0 )
    {

      return false
    }
    if (regex_redirect.findAllIn(v).length >0 )
    {

      return false
    }
    if (regex_Disam.findAllIn(v).length >0 ) {

      return false
    }
    else{

    return true}

  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Wiki WordCount")
    val sc = new SparkContext(sparkConf)
    //val txt = sc.textFile("outputfile2")

    val txt = sc.textFile("outputfile")
    //println("here")
    //map works by apply a function to each element in the RDD
    val getTitleAndText = txt.map { I =>
      val line = XML.loadString(I)
      val title = (line \ "title").text
      val text = (line \\ "text").text
      (title, text)
    }
    //println("here2")
    //getTitleAndText.collect().foreach(println)

    //println("I am here")
    // getTitleAndText.collect.filter(r => isArticle(r._2.toLowerCase))
    val articles = getTitleAndText.filter(r=>isArticle(r._2.toLowerCase))
    //println("here3")
    //get number of articles
    val count = articles.count

    //seperate by tab
    val tapSeperate_articles = articles.map(f => (f._1 +"\t"+f._2))

    //pritn count
    println(count)
    //tapSeperate_articles.collect.foreach(println)

    //save file
    tapSeperate_articles.saveAsTextFile("articles_official2")
  }




}
