package com.atguigu.flink.chapter09;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/3/9 9:00
 */
public class Flink01_CEP_BaseUse {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 1. 先有数据流
        // 返回事件时间
        SingleOutputStreamOperator<WaterSensor> stream = env
                .fromElements(
                        new WaterSensor("sensor_1", 1L, 10),
                        new WaterSensor("sensor_1", 2L, 20),
                        new WaterSensor("sensor_2", 3L, 30),
                        new WaterSensor("sensor_1", 4L, 40),
                        new WaterSensor("sensor_2", 5L, 50),
                        new WaterSensor("sensor_1", 6L, 60)
                )
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
                });

        // 3. 把规则作用到流上, 得到一个模式流
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);
        // 4. 从模式流中取出匹配到的数据
        ps
                .select(new PatternSelectFunction<WaterSensor, String>() {
                    // 每匹配成功一次, 这个方法就执行一次
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        return map.get("s1").toString();

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
