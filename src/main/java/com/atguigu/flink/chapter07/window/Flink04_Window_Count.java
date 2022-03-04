package com.atguigu.flink.chapter07.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/4 14:24
 */
public class Flink04_Window_Count {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);


        env
                .socketTextStream("hadoop162", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value,
                                        Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                //.countWindow(3)
                // 每来一个元素触发一次计算, 窗口内的元素的个数最多是3
                .countWindow(3, 1)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Long> t : elements) {
                            sum++;
                        }
                        out.collect("key = " + key + ", 个数: " + sum);
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
