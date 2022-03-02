package com.atguigu.flink.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink07_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 6);
        DataStreamSource<String> s2 = env.fromElements("a", "b", "c", "d", "e", "f");

        ConnectedStreams<Integer, String> s12 = s1.connect(s2);

        s12
                .map(new CoMapFunction<Integer, String, String>() {


                    // 处理第一个流的元素
                    @Override
                    public String map1(Integer integer) throws Exception {
                        return integer + "<";
                    }

                    // 处理第二个流的元素
                    @Override
                    public String map2(String s) throws Exception {
                        return s + ">";
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
 */