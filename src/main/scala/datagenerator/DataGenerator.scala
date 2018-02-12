package  datagenerator
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import java.io.FileWriter
import java.time.LocalDate

import scala.collection.JavaConversions._

object DataGenerator {
  def main(args: Array[String]) {
    var outFile = "purchases.csv"
    var geoData = "GeoLite2-Country-Blocks-IPv4.csv"
    var lines = 100

    // Parse args by pairs
    args.sliding(2, 2).toList.collect {
      case Array("--out", out: String) => outFile = out
      case Array("--geo", geo: String) => geoData = geo
      case Array("--lines", num: String) => lines = num.toInt
    }

    var items = Array("Shirt", "Pants", "Hoodie", "Sweater", "Jacket", "Coat", "Jeans", "Shorts", "Slippers", "Boots", "Shoes")
    var cats = Array("Tops", "Bottoms", "Shoes", "Accessories")

    var num = new Random(System.currentTimeMillis())

    def getItem[T](data: Array[T]): T = {
      /**
        *  @return Random element of Array
        */
      var random_index = num.nextInt(data.length)
      data(random_index)
    }

    def getIp():String = {
      /**
        * @return String, containing random IP address
        */
      def ip():Int = 1 + num.nextInt(253)
      ip + "." + ip + "." + ip + "." + ip
    }

    def getPrice():String = {
      /** Random price
        *  @return String
        */
      "%.2f".format(100 + num.nextGaussian()*33.333)
    }

    def getDate():String = {
      /**
        * random date within one week
        * @return String
        */
      val from = LocalDate.of(2017, 8, 6)
      val to = LocalDate.of(2017, 8 ,13)
      LocalDate.ofEpochDay(from.toEpochDay + Random.nextInt((to.toEpochDay - from.toEpochDay).toInt)).toString
    }

    def getGausianLimited():Double = {
      var res: Double = 0
      do {
          res = num.nextGaussian()
        } while ((res > 2) || (res < -2))
      res
    }

    def getTime():String = {
      /** Get random time
        * @return
        */
      "%s:%s".format(((getGausianLimited() * 4) + 12).toInt, ((getGausianLimited() * 15) + 30).toInt)
    }


    val writer = new CSVWriter(new FileWriter(outFile), '\t');
    for( a <- 1 to lines){
      val entry = Array(getDate() + " " + getTime(), getIp(), getItem(items), getItem(cats), getPrice())
      writer.writeNext(entry);

//      println(getDate() + " - " + getTime() + "  " + getIp() + "  " + getItem(items) + " in category " + getItem(cats) + " costs " + getPrice() + "\n" );
    }

    writer.close();

  }
}