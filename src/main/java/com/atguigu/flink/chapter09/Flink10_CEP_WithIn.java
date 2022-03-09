package com.atguigu.flink.chapter09;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/3/9 9:00
 */
public class Flink10_CEP_WithIn {
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
                })
                .next("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                })
                .within(Time.seconds(2));


        // 3. 把规则作用到流上, 得到一个模式流
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);
        // 4. 从模式流中取出匹配到的数据
        SingleOutputStreamOperator<String> normal = ps
                .select(
                        new OutputTag<WaterSensor>("timeout") {
                        },
                        new PatternTimeoutFunction<WaterSensor, WaterSensor>() {
                            // 参数就是匹配的时候, 超时的数据
                            // 返回值,就是不要放入侧输出流的数据
                            @Override
                            public WaterSensor timeout(Map<String, List<WaterSensor>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.get("s1").get(0);
                            }
                        },
                        new PatternSelectFunction<WaterSensor, String>() {

                            @Override
                            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                                return pattern.toString();
                            }

                        });

        normal.print("匹配成功...");
        normal.getSideOutput( new OutputTag<WaterSensor>("timeout") {}).print("timeout...");
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