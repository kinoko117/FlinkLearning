package com.atguigu.flink.chapter02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/2/28 10:15
 */
public class Flink03_Wc_UnBounded_Lambda {
    public static void main(String[] args) throws Exception {
        System.out.println("main.............");
        // 1. 创建一个流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 通过env得到一个数据流
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop162", 9999);
        // 3. 各种转换
        // 不会更改流中的数据结构, 仅仅是对元素进行分组
        SingleOutputStreamOperator<Tuple2<String, Long>> result = dataStreamSource
            .flatMap((String line, Collector<String> collector) -> {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }).returns(String.class)
            .map((String word) -> Tuple2.of(word, 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(t -> t.f0)
            .sum(1);// 对tuple2中位置是1的元素做聚合
        
        result.print();
        
        env.execute();  // 执行环境
        
    }
}

/*
在flink中, 运行的时候, flink框架不要明确知道泛型的类型
1. 使用匿名内部类  推荐
2. 使用外部类 当抽象方法实现特别复杂
3. 使用lambda, 但是必须要明确的指定泛型信息  不推荐

 */

