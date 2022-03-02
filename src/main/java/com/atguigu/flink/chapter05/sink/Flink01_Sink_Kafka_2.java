package com.atguigu.flink.chapter05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/3/2 11:14
 */
public class Flink01_Sink_Kafka_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 4L, 30),
                new WaterSensor("sensor_1", 2L, 20),
                new WaterSensor("sensor_1", 5L, 40),
                new WaterSensor("sensor_2", 4L, 100),
                new WaterSensor("sensor_2", 5L, 200)
        );

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092");
        stream
                .addSink(new FlinkKafkaProducer<WaterSensor>(
                        "default",
                        new KafkaSerializationSchema<WaterSensor>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WaterSensor element,
                                                                            @Nullable Long timestamp) {
                                return new ProducerRecord<>(element.getId(), element.toString().getBytes(StandardCharsets.UTF_8));
                            }
                        },
                        props,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
                ));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
