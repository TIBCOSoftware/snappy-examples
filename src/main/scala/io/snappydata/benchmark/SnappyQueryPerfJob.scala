package io.snappydata.benchmark

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql._

class SnappyQueryPerfJob extends SnappySQLJob {

  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Any = {
    val outFileName = s"QueryPerf-${System.currentTimeMillis()}.out"
    val pw = new PrintWriter(outFileName)
    var start = System.currentTimeMillis()
    sc.sql("select count(*) AS adCount, geo from adImpressions group by geo order by adCount desc limit 20").collect()
    pw.println("Time for Q1 " + (System.currentTimeMillis() - start))
    pw.flush()

    start = System.currentTimeMillis()
    sc.sql("select sum (bid) as max_bid, geo from adImpressions group by geo order by max_bid desc limit 20").collect()
    pw.println("Time for Q2 " + (System.currentTimeMillis() - start))
    pw.flush()

    start = System.currentTimeMillis()
    sc.sql("select sum (bid) as max_bid, publisher from adImpressions group by publisher order by max_bid desc limit 20").collect()
    pw.println("Time for Q3 " + (System.currentTimeMillis() - start))
    pw.flush()

    start = System.currentTimeMillis()
    val array = sc.sql("select count(*) from adImpressions").collect()
    pw.println(array(0) +"Time for count(*) " + (System.currentTimeMillis() - start))
    pw.flush()
    pw.close()
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}
