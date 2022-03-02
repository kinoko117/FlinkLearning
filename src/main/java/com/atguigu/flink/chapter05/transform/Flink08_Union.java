package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink08_Union {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 6);
        DataStreamSource<Integer> s2 = env.fromElements(10, 20, 30);


        DataStream<Integer> stream = s1.union(s2);
        stream
                .map(new MapFunction<Integer, String>() {
                    @Override
                    public String map(Integer value) throws Exception {
                        return value + ">";
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
connect:
1. 只能连接两个流
2. 这两个流的类型可以不一样

Union:
1. 可以多个流union在一起
2. 类型必须一样
 */