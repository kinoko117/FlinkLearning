package com.atguigu.flink.chapter05.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2022/3/1 14:52
 */
public class Flink02_Filter {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);
    
        stream
            .filter(new FilterFunction<Integer>() {
                @Override
                public boolean filter(Integer value) throws Exception {
                    return value % 2 == 0;
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
