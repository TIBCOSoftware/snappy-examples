package io.snappydata.benchmark.chbench

import java.util.Random
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class BenchmarkingReceiver(val maxRecPerSecond: Int,
                           val numWarehouses: Int,
                           val numDistrictsPerWarehouse: Int,
                           val numCustomersPerDistrict: Int,
                           val itemCount: Int)
  extends Receiver[ClickStreamCustomer](StorageLevel.MEMORY_AND_DISK_2) {


  var receiverThread: Thread = null
  var stopThread = false;

  override def onStart() {
    receiverThread = new Thread("BenchmarkingReceiver") {
      override def run() {
        receive()
      }
    }
    receiverThread.start()
  }

  override def onStop(): Unit = {
    receiverThread.interrupt()
  }

  private def receive() {
    while (!isStopped()) {
      val start = System.currentTimeMillis()
      var i = 0;
      for (i <- 1 to maxRecPerSecond) {
        store(generateClickStream())
        if (!isStopped()) {
          return
        }
      }
      // If one second hasn't elapsed wait for the remaining time
      // before queueing more.
      val remainingtime = 1000 - (System.currentTimeMillis() - start)
      if (remainingtime > 0) {
        Thread.sleep(remainingtime)
      }
    }
  }

  val rand = new Random(123)

  private def generateClickStream(): ClickStreamCustomer = {
    val warehouseID: Int = rand.nextInt(numWarehouses)
    val districtID: Int = rand.nextInt(this.numDistrictsPerWarehouse)
    val customerID: Int = rand.nextInt(this.numCustomersPerDistrict)
    val itemId: Int = rand.nextInt(this.itemCount)
    new ClickStreamCustomer(warehouseID, districtID, customerID, itemId)
  }
}

class ClickStreamCustomer(val w_id: Int,
                          val d_id: Int,
                          val c_id: Int,
                          val i_id: Int)