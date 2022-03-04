package com.atguigu.flink.chapter07.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Random;

/**
 * @Author lzc
 * @Date 2022/3/4 14:24
 */
public class Flink03_Window_Session {
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
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extract(Tuple2<String, Long> element) {
                        return new Random().nextInt(20*1000);
                    }
                }))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    // 窗口关闭的(时间大于等于窗口结束时间)时候触发计算
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<Tuple2<String, Long>> elements, // 存储了窗口内所有元素
                                        Collector<String> out) throws Exception {

                        Date start = new Date(ctx.window().getStart());
                        Date end = new Date(ctx.window().getEnd());

                        int count = 0;
                        for (Tuple2<String, Long> element : elements) {
                            count++;
                        }

                        out.collect("key = " + key + ", 窗口开始: " + start + ", 窗口结束: " + end + ", 个数: " + count);

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
