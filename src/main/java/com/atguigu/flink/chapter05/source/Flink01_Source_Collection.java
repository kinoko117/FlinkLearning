package com.atguigu.flink.chapter05.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 11:37
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        ArrayList<String> list = new ArrayList<>();
        list.add("a");
        list.add("a1");
        list.add("a2");
        list.add("a3");
        list.add("a4");
    
//        DataStreamSource<String> stream = env.fromCollection(list);
//        stream.print();
        env.fromElements("a", "b", "c", "d").print();
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
