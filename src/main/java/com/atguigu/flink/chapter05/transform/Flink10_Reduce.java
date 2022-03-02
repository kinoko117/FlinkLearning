package com.atguigu.flink.chapter05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink10_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
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

        stream
                .keyBy(WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1,  // 上次聚合的结果
                                              WaterSensor value2) throws Exception {  // 本次需要参与聚合的元素
//                        System.out.println("Flink10_Reduce.reduce");
//                        value1.setVc(value1.getVc() + value2.getVc());
                        value1.setVc(Math.max(value1.getVc(), value2.getVc()));

                        return value1;
                    }
                })
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
reduce聚合算子:
    输入类型和聚合后的结果类型是一致!!!


 */