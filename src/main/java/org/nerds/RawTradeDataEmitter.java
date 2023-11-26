package org.nerds;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.nerds.Utils.TOPIC;

public class RawTradeDataEmitter {
    private static final Logger logger = LoggerFactory.getLogger(RawTradeDataEmitter.class);
    private static final List<TradeData> EVENTS = Collections.synchronizedList(new ArrayList<>());
    private static final Lock lock = new ReentrantLock();

    public static void main(String[] args) throws Exception {
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new KafkaTradeEventProducer(), 0, 5000);
        logger.info("Waiting for 10s");
        Thread.sleep(10000);
        var client = MyWebSocketClient.getInstance();
        client.connect();
        new Thread(() -> {
            while (true) {
                if (client.isConnected()) break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            logger.info("Sending Info...");
            client.send("{\"type\":\"subscribe\",\"symbol\":\"BINANCE:BTCUSDT\"}".getBytes(StandardCharsets.UTF_8));
            client.send("{\"type\":\"subscribe\",\"symbol\":\"AMZN\"}".getBytes(StandardCharsets.UTF_8));
            client.send("\"type\":\"subscribe\",\"symbol\":\"AAPL\"}".getBytes(StandardCharsets.UTF_8));
            client.send("{\"type\":\"subscribe\",\"symbol\":\"IC MARKETS:1\"}".getBytes(StandardCharsets.UTF_8));
        }).start();
    }


    private static class KafkaTradeEventProducer extends TimerTask {
        private static final Logger kafkaLogger = LoggerFactory.getLogger(RawTradeDataEmitter.class);
        private final KafkaConnection kafkaConnection = KafkaConnection.getInstance();

        @Override
        public void run() {
            logger.info("entering run emitter...");
            List<TradeData> events = new ArrayList<>();
            synchronized (EVENTS) {
                lock.lock();
                try {
                    if (!EVENTS.isEmpty()) {
                        events.addAll(EVENTS);
                        EVENTS.clear();
                    }
                } finally {
                    lock.unlock();
                }
            }
            if (!events.isEmpty())
                emit(events);
        }

        private void emit(List<TradeData> events) {
            //emit kafka message
            Gson gson = new Gson();
            String jsonString = gson.toJson(events);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, "data", jsonString.getBytes(StandardCharsets.UTF_8));
            kafkaConnection.getProducer().send(record, (metadata, exception) -> {
                if (exception == null) {
                    kafkaLogger.info("Message send Successfully");
                } else {
                    kafkaLogger.error("Failed to send message", exception);
                }
            });
        }
    }


    private static class MyWebSocketClient extends WebSocketClient {
        private static final Logger socketLogger = LoggerFactory.getLogger(MyWebSocketClient.class);
        private final AtomicBoolean isConnected = new AtomicBoolean(false);


        private MyWebSocketClient() throws URISyntaxException, IOException {
            super(new URI(Utils.getEndpoint()));
        }

        private static MyWebSocketClient instance;

        public static MyWebSocketClient getInstance() throws URISyntaxException, IOException {
            synchronized (MyWebSocketClient.class) {
                if (instance == null) instance = new MyWebSocketClient();
            }
            return instance;
        }

        public synchronized boolean isConnected() {
            return isConnected.get();
        }


        @Override
        public void onOpen(ServerHandshake serverHandshake) {
            socketLogger.info("Connected");
            isConnected.set(true);
        }


        @Override
        public void onMessage(String s) {
//            socketLogger.info("Received Info = " + s);

            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(s, JsonObject.class);
            JsonArray dataArray = jsonObject.getAsJsonArray("data");
            if (dataArray != null && !dataArray.isEmpty()) {
                // Collect and send as kafka notification
                synchronized (EVENTS) {
                    lock.lock();
                    try {
                        EVENTS.addAll(Arrays.stream(gson.fromJson(dataArray, TradeData[].class)).toList());
                    } finally {
                        lock.unlock();
                    }
                }
            }

            logger.info("Here is the EVENTS Size = " + EVENTS.size());
        }

        @Override
        public void onClose(int i, String s, boolean b) {
            socketLogger.info("Closing....");
            isConnected.set(false);
        }

        @Override
        public void onError(Exception e) {
            socketLogger.error("", e);
        }
    }
}
