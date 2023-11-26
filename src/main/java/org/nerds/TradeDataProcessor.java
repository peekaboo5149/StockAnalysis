package org.nerds;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.nerds.Utils.TOPIC;

public class TradeDataProcessor {

    public static void main(String[] args) {
        KafkaConnection kafkaConnection = KafkaConnection.getInstance();
        final KafkaConsumer<String, byte[]> consumer = kafkaConnection.getConsumer();
        Gson gson = new Gson();
        consumer.subscribe(Collections.singleton(TOPIC));
        List<TradeData> rawDatas = new ArrayList<>();
        do {
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
        } while (true);
// do some analysis
    }

}
