package org.rahkarsanat;


import com.kevin.flink.streaming.connectors.mqtt.MQTTMessage;
import com.kevin.flink.streaming.connectors.mqtt.MQTTStreamSource;
import com.kevin.flink.streaming.connectors.mqtt.internal.MQTTExceptionListener;
import com.kevin.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Map<String, String> sourceConfig = new HashMap<String, String>();
    private static String  MQTT_SERVER_ADDRESS = new String("tcp://mqtt.rahkarsanat.org:1883");
    private static String  MQTT_SERVER_USERNAME = new String("developer");
    private static String  MQTT_SERVER_PASSWORD = new String("");

    public static void main(String[] args) throws Exception {

        LOG.info("Starting Flink Mqtt Stream Processing Engine!");

        sourceConfig.put("clientId", "flinkMqttSource");
        sourceConfig.put("topic", "gps/v1/#");
        sourceConfig.put("brokerUrl", MQTT_SERVER_ADDRESS);
        sourceConfig.put("username", MQTT_SERVER_USERNAME);
        sourceConfig.put("password", MQTT_SERVER_PASSWORD);
        sourceConfig.put("QoS", "2");
        sourceConfig.put("persistence", "memory");


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MQTTStreamSource mqtt = new MQTTStreamSource(sourceConfig);
        mqtt.setLogFailuresOnly(true);
        MQTTExceptionListener exceptionListener = new MQTTExceptionListener(LOG, true);
        mqtt.setExceptionListener(exceptionListener);
        mqtt.setRunningChecker(new RunningChecker());
        DataStream<MQTTMessage> stream = env.addSource(mqtt);
        DataStream<MQTTMessage> dataStream = stream
                .flatMap(new FlatMapFunction<MQTTMessage, MQTTMessage>() {
                    @Override
                    public void flatMap(MQTTMessage message, Collector<MQTTMessage> out) throws Exception {
                        System.out.println(new String(message.getPayload()));
                        LOG.info(new String(message.getPayload()));
                        out.collect(message);
                    }

                }).setParallelism(1);

        dataStream.print().setParallelism(1);
        env.execute("MQTT Stream");
    }
}