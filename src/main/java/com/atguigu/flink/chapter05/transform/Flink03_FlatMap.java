package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink03_FlatMap {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);
        // 1->N(0,1, >1)
        stream
            .flatMap(new FlatMapFunction<Integer, Integer>() {
                @Override
                public void flatMap(Integer value,
                                    Collector<Integer> out) throws Exception {
                    out.collect(value * value);
                    //out.collect(value * value * value);
                    
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
flatMap可以替换 map和filter
 */