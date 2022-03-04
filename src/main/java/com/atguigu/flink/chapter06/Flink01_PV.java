package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/3/4 8:56
 */
public class Flink01_PV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4])
                    );
                })
                .filter(ub -> "pv".equals(ub.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of("pv", 1L);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
