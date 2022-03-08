package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/3/8 9:55
 */
public class Flink01_PV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4]) * 1000);
                })
                .filter(ub -> "pv".equals(ub.getBehavior()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ub, ts) -> ub.getTimestamp())

                )
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior value, Long acc) {
                        return acc + 1;
                    }

                    @Override
                    public Long getResult(Long acc) {
                        return acc;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
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
