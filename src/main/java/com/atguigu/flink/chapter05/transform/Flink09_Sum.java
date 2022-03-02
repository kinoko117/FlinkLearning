package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink09_Sum {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 2L, 20),
                new WaterSensor("sensor_1", 4L, 40),
                new WaterSensor("sensor_2", 4L, 100),
                new WaterSensor("sensor_2", 5L, 200)
        );

        // 计算每个传感器的水位和
        // select id, "abc", sum(vc) from t group by id

        stream
//                .keyBy( value -> value.getId())
                .keyBy(WaterSensor::getId)
//                .sum("vc")
//                .max("vc")
                .min("vc")
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
除了分组字段和聚合字段, 其他字段的值是取的第一个

只能针对数字进行聚合
sum max min
 */