package com.thecodinginterface.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

/**
 * Producer app writing single Order records at a time to a Kinesis Data Stream using
 * the PutRecord API of the JAVA SDK.
 *
 * The globally unique Order ID of each record is used as the partition key which ensures
 * order records will be equally distributed across the shard of the stream.
 */
public class PutRecordApp {
    static final Logger logger = LogManager.getLogger(PutRecordApp.class);
    static final ObjectMapper objMapper = new ObjectMapper();

    public static void main( String[] args ) {
        logger.info("Starting PutRecord Producer");
        String streamName = args[0];

        // Instantiate the client
        var client = KinesisClient.builder().build();

        // Add shutdown hook to cleanly close the client when program terminates
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down program");
            client.close();
        }, "producer-shutdown"));

        while (true) {
            // Generate fake order data
            var order = OrderGenerator.makeOrder();
            logger.info(String.format("Generated %s", order));

            try {
                // construct single PutRecord request
                var putRequest = PutRecordRequest.builder()
                                        .partitionKey(order.getOrderID())
                                        .streamName(streamName)
                                        .data(SdkBytes.fromByteArray(objMapper.writeValueAsBytes(order)))
                                        .build();

                // execute single PutRecord request
                PutRecordResponse response = client.putRecord(putRequest);
                logger.info(String.format("Produced Record %s to Shard %s", response.sequenceNumber(), response.shardId()));
            } catch(JsonProcessingException e) {
                logger.error(String.format("Failed to serialize %s", order), e);
            } catch (KinesisException e) {
                logger.error(String.format("Failed to produce %s", order), e);
            }

            // introduce artificial delay for demonstration purpose / visual tracking of logging
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
