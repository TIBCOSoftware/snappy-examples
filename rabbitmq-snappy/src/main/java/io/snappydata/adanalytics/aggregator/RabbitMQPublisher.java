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

package io.snappydata.adanalytics.aggregator;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A RabbitMQ client which publishes messages to RabbitMQ server
 */
public class RabbitMQPublisher {

    private final static String QUEUE_NAME = "rabbitmq-q";

    public static void main(String[] argv)
            throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        int logCount = 0;
        int totalNumLogs = 1000000;

        while (logCount <= totalNumLogs) {
            AdImpressionLog log = KafkaAdImpressionGenerator.generateAdImpression();
            channel.basicPublish("", QUEUE_NAME, null, getLogBytes(log));
            logCount += 1;
            if (logCount % 100000 == 0) {
                System.out.println("RabbitMQPublisher published total " +
                        logCount + " messages");
            }
        }

        channel.close();
        connection.close();
    }

    private static byte[] getLogBytes(AdImpressionLog log) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<AdImpressionLog> writer = new
                SpecificDatumWriter(AdImpressionLog
                .getClassSchema());
        writer.write(log, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }
}
