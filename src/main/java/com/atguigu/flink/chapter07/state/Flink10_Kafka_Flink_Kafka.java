package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @Author lzc
 * @Date 2022/3/7 9:12
 */
public class Flink10_Kafka_Flink_Kafka {
    public static void main(String[] args) throws IOException {
        //shift + ctrl + u
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        /*
        从socket读数据, 存入到一个ArrayList
        如何把ArrayList中的数据存入到列表状态, 并在程序恢复能够从状态把数据再恢复到ArrayList
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // 开启checkpoint
        env.enableCheckpointing(2000);
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck10");

        // 设置checkpoint的一致性
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置同时并发的checkpoint的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 两个checkpoint之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10 * 1000);
        // 程序被取消之后, checkpoint数据仍然保留
//        env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        Properties sourceProps = new Properties();
        sourceProps.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sourceProps.put("group.id", "Flink10_Kafka_Flink_Kafka");
        sourceProps.put("auto.reset.offset", "latest"); // 如果没有上次的 消费记录就从最新位置消费, 如果有记录, 则从上次的位置开始消费


        Properties sinkProps = new Properties();
        sinkProps.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sinkProps.put("transaction.timeout.ms", 15 * 60 * 1000);

        // TODO
        env
                .addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), sourceProps))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));

                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .addSink(new FlinkKafkaProducer<Tuple2<String, Long>>(
                        "default",
                        new KafkaSerializationSchema<Tuple2<String, Long>>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element,
                                                                            @Nullable Long timestamp) {
                                String msg = element.f0 + "_" + element.f1;
                                return new ProducerRecord<>("s2", msg.getBytes(StandardCharsets.UTF_8));
                            }
                        },
                        sinkProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
