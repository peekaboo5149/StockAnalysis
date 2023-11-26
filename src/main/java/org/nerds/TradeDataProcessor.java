package org.nerds;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.nerds.Utils.TOPIC;

public class TradeDataProcessor {
    private final static Logger logger = LoggerFactory.getLogger(TradeDataProcessor.class);

    public static void main(String[] args) {
        KafkaConnection kafkaConnection = KafkaConnection.getInstance();
        final KafkaConsumer<String, byte[]> consumer = kafkaConnection.getConsumer();
        Gson gson = new Gson();
        consumer.subscribe(Collections.singleton(TOPIC));
        List<TradeData> rawDatas = new ArrayList<>();
        do {
            try {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    final String value = new String(record.value());
                    JsonArray array = gson.fromJson(value, JsonArray.class);
                    System.out.println("data" + array.toString());
                    for (int i = 0; i < array.size(); i++) {
                        JsonObject json = array.get(i).getAsJsonObject();
                        rawDatas.add(gson.fromJson(json, TradeData.class));
                    }

                });
            } catch (Exception e) {
                logger.error("", e);
                break;
            }
        } while (true);
// do some analysis
    }

}
