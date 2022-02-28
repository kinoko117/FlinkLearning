package com.atguigu.flink.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/2/28 10:15
 */
public class Flink01_Wc_Bounded {
    public static void main(String[] args) throws Exception {
        // 1. 创建一个流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 通过env得到一个数据流
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");
        // 3. 各种转换
        SingleOutputStreamOperator<Tuple2<String, Long>> result = dataStreamSource
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String line,
                                    Collector<String> collector) throws Exception {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(word);
                    }
                }
            })
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String word) throws Exception {
                    return Tuple2.of(word, 1L);
                }
            })
            .keyBy(new KeySelector<Tuple2<String, Long>, String>() { // 不会更改流中的数据结构, 仅仅是对元素进行分组
                @Override
                public String getKey(Tuple2<String, Long> t) throws Exception {
                    return t.f0;
                }
            })
            .sum(1);// 对tuple2中位置是1的元素做聚合
    
        result.print();
    
        env.execute();  // 执行环境
    
    }
}
/*
spark rdd:
    1. 创建一个上下文对象
    2. 通过上下文对象得到一个rdd
    3. 对rdd做各种转换
    4. 行动算子
    5. 启动上下文对象

 */