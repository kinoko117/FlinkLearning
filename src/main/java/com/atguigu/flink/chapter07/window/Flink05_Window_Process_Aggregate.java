package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/4 14:24
 */
public class Flink05_Window_Process_Aggregate {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);


        env
                .socketTextStream("hadoop162", 9999)
                .map(value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<WaterSensor, Avg, Double>() {
                            // 初始化一个累加器
                            // 第一个数据来到时候触发
                            @Override
                            public Avg createAccumulator() {
                                System.out.println("Flink05_Window_Process_Aggregate.createAccumulator");
                                return new Avg();
                            }

                            // 对进来的元素进行累加
                            // 每来一条数据触发一次
                            @Override
                            public Avg add(WaterSensor value, Avg acc) {
                                System.out.println("Flink05_Window_Process_Aggregate.add");
                                acc.vcSum += value.getVc();
                                acc.count++;
                                return acc;
                            }

                            // 返回最终的结果
                            // 窗口关闭的时候触发一次
                            @Override
                            public Double getResult(Avg acc) {
                                System.out.println("Flink05_Window_Process_Aggregate.getResult");
                                return acc.vcSum * 1.0 / acc.count;
                            }

                            // 合并累加器
                            // 一般不用实现: 如果窗口是session窗口, 才需要实现
                            // 其他窗口不会触发
                            @Override
                            public Avg merge(Avg a, Avg b) {
                                System.out.println("Flink05_Window_Process_Aggregate.merge");
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<Double> elements,
                                                Collector<String> out) throws Exception {
                                Double result = elements.iterator().next();
                                out.collect(key + "的平均值: " + result + "  " + ctx.window());
                            }
                        }
                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Avg {
        public Integer vcSum = 0;
        public Long count = 0L;

    }
}
/*
窗口处理函数:

增量
    sum max min maxBy minBy

    reduce

    aggregate

全量
    process



 */