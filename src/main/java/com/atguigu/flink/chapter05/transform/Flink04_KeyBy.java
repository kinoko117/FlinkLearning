package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink04_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);
        stream
            /*    .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 == 0 ? "偶数" : "奇数";
                    }
                })
                .print();*/
            .keyBy(new KeySelector<Integer, Integer>() {
                @Override
                public Integer getKey(Integer value) throws Exception {
                    return value % 2;
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