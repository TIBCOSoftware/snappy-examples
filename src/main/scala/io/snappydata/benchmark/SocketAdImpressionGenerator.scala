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

import java.io.{ByteArrayOutputStream, IOException}
import java.net.ServerSocket

import io.snappydata.adanalytics.Configs._
import io.snappydata.adanalytics.{AdImpressionGenerator, AdImpressionLog}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.streaming.StreamUtils

/**
  * A Simple program which writes Avro objects to socket stream
  */
object SocketAdImpressionGenerator extends App {

  val bytesPerSec = 800000000
  val blockSize = bytesPerSec / 10
  val bufferStream = new ByteArrayOutputStream(blockSize + 1000)
  val encoder = EncoderFactory.get.directBinaryEncoder(bufferStream, null)
  val writer = new SpecificDatumWriter[AdImpressionLog](
    AdImpressionLog.getClassSchema)
  while (bufferStream.size < blockSize) {
    writer.write(AdImpressionGenerator.nextRandomAdImpression, encoder)
  }
//  encoder.flush
//  bufferStream.close

  val serverSocket = new ServerSocket(socketPort)
  println("Listening on port " + socketPort)

  while (true) {
    val socket = serverSocket.accept()
    println("Got a new connection")
    val out = StreamUtils.getRateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
    try {
      while (true) {
        out.write(bufferStream.toByteArray)
        //out.flush
      }
    } catch {
      case e: IOException =>
        println("Client disconnected")
        socket.close()
    }
  }
}

/*
final class Server(port: Int) extends Runnable {
  def run(): Unit = {
    launchServerSockets(port)
  }

  private def launchServerSockets(port: Int): Unit = {
    val serverSocket = new ServerSocket(port, 50, InetAddress.getLocalHost)
    println("Listening on port " + port)

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection on "+ port)
      try {
        while (true) {
          val threads = new Array[Thread](1)
          for (i <- 0 until 1) {
            val thread = new Thread(new Ingester(socket))
            thread.start()
            threads(i) = thread
          }
          threads.foreach(_.join())
        }
      } catch {
        case e: IOException =>
          println("Client disconnected from port " + port)
          socket.close()
      }
    }
  }
}

final class Ingester(socket: Socket) extends Runnable {
  def run() {
    for (i <- 0 until 100000) {
      val out = new ByteArrayOutputStream(630000) //1000 AdImpressions
      val encoder = EncoderFactory.get.directBinaryEncoder(out, null)
      val writer = new SpecificDatumWriter[AdImpressionLog](
        AdImpressionLog.getClassSchema)
      while (out.size < 630000) {
        writer.write(generateAdImpression, encoder)
      }
      encoder.flush
      out.close
      writeToSocket(socket, out.toByteArray)
    }
  }

  private def writeToSocket(socket: Socket, bytes: Array[Byte]) = synchronized {
    socket.getOutputStream.write(bytes)
    socket.getOutputStream.flush()
  }

  private def generateAdImpression(): AdImpressionLog = {
    val numPublishers = 50
    val numAdvertisers = 30
    val publishers = (0 to numPublishers).map("publisher" +)
    val advertisers = (0 to numAdvertisers).map("advertiser" +)
    val unknownGeo = "un"

    val geos = Seq("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL",
      "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
      "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
      "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
      "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", unknownGeo)

    val numWebsites = 999
    val numCookies = 999
    val websites = (0 to numWebsites).map("website" +)
    val cookies = (0 to numCookies).map("cookie" +)

    val random = new java.util.Random()
    val timestamp = System.currentTimeMillis()
    val publisher = publishers(random.nextInt(numPublishers - 10 + 1) + 10)
    val advertiser = advertisers(random.nextInt(numAdvertisers - 10 + 1) + 10)
    val website = websites(random.nextInt(numWebsites - 100 + 1) + 100)
    val cookie = cookies(random.nextInt(numCookies - 100 + 1) + 100)
    val geo = geos(random.nextInt(geos.size))
    val bid = math.abs(random.nextDouble()) % 1

    val log = new AdImpressionLog()
//    log.setTimestamp(1L)
//    log.setPublisher("publisher")
//    log.setAdvertiser("advertiser")
//    log.setWebsite("website")
//    log.setGeo("geo")
//    log.setBid(1D)
//    log.setCookie("cookie")
    log.setTimestamp(timestamp)
    log.setPublisher(publisher)
    log.setAdvertiser(advertiser)
    log.setWebsite(website)
    log.setGeo(geo)
    log.setBid(bid)
    log.setCookie(cookie)
    log
  }
}
*/
