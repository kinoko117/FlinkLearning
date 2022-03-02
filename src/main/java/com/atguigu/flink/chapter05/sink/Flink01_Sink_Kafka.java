package com.atguigu.flink.chapter05.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/2 11:14
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("hadoop162", 9999)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(word);

                        }
                    }
                })
                .addSink(
                        new FlinkKafkaProducer<String>(
                                "hadoop162:9092,hadoop163:9092",
                                "s2",
                                new SimpleStringSchema()
                        )
                );


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
