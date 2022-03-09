package com.atguigu.flink.chapter09;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/3/9 9:00
 */
public class Flink11_CEP_Select {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 1. 先有数据流
        // 返回事件时间
        SingleOutputStreamOperator<WaterSensor> stream = env
                .readTextFile("input/sensor.txt")
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))  // 乱序不程度
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())

                );

        // 2. 指定规则(定义模式)
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).times(2).consecutive()
                .next("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                });


        // 3. 把规则作用到流上, 得到一个模式流
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);
        // 4. 从模式流中取出匹配到的数据
        ps
                .flatSelect(new PatternFlatSelectFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void flatSelect(Map<String, List<WaterSensor>> map,
                                           Collector<WaterSensor> out) throws Exception {
                        for (WaterSensor ws : map.get("s1")) {
                            out.collect(ws);

                        }

                        for (WaterSensor ws : map.get("s2")) {
                            out.collect(ws);

                        }

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
cep匹配的时候一共有三种数据:
1. 匹配上的数据   可以得到
2. 不匹配的数据 无法获取到, flink直接放弃
3. 超时数据  可以得到
 */