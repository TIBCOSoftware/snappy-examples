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
package io.snappydata.adanalytics.aggregator

import java.io.{ByteArrayOutputStream, IOException}
import java.net.ServerSocket
import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

/**
  * A Simple program which writes Avro objects to socket stream
  */
class SocketAdImpressionGenerator extends AdImpressionGenerator {
  val bytesPerSec = 10000000
  val blockSize = bytesPerSec / 10
  val bufferStream = new ByteArrayOutputStream(blockSize + 1000)
  val ser = new KryoSerializer(new SparkConf()).newInstance()
  val serStream = ser.serializeStream(bufferStream)
  while (bufferStream.size < blockSize) {
    serStream.writeObject(generateAdImpression)
  }
  val array = bufferStream.toByteArray
  val countBuf = ByteBuffer.wrap(new Array[Byte](4))
  countBuf.putInt(array.length)
  countBuf.flip()

  val serverSocket = new ServerSocket(9002)
  println("Listening on port " + 9002)

  while (true) {
    val socket = serverSocket.accept()
    println("Got a new connection")
    // val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
    try {
      while (true) {
       // out.write(countBuf.array)
       // out.write(array)
      }
    } catch {
      case e: IOException =>
        println("Client disconnected")
        socket.close()
    }
  }
}
