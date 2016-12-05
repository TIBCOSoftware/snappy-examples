/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package io.snappydata.benchmark

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

class SnappySampleQueryPerfJob extends SnappySQLJob {

  override def runSnappyJob(sc: SnappyContext, jobConfig: Config): Any = {
    val outFileName = s"SampleQueryPerf-${System.currentTimeMillis()}.out"
    val pw = new PrintWriter(outFileName)
    var start = System.currentTimeMillis()
    sc.sql("select count(*) AS adCount, geo from adImpressions group by geo" +
      " order by adCount desc limit 20 with error 0.1").collect()
    pw.println("Time for Sample Q1 " + (System.currentTimeMillis() - start))
    pw.flush()

    start = System.currentTimeMillis()
    sc.sql("select sum (bid) as max_bid, geo from adImpressions group by geo" +
      " order by max_bid desc limit 20 with error 0.1").collect()
    pw.println("Time for Sample Q2 " + (System.currentTimeMillis() - start))
    pw.flush()

    start = System.currentTimeMillis()
    val array = sc.sql("select count(*) as sample_cnt from" +
      " adImpressions with error 0.1").collect()
    pw.println(array(0) +"Time for sample count(*) " + (System.currentTimeMillis() - start))
    pw.flush()

    start = System.currentTimeMillis()
    sc.sql("select sum (bid) as max_bid, publisher from adImpressions group by" +
      " publisher order by max_bid desc limit 20 with error 0.5").collect()
    pw.println("Time for Sample Q3 " + (System.currentTimeMillis() - start))
    pw.flush()

    pw.close()
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}
