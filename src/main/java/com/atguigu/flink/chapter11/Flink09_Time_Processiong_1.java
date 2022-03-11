package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink09_Time_Processiong_1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table sensor(" +
                "   id string," +
                "   ts bigint, " +
                "   vc int, " +
                "   pt as proctime() " +
                ")with(" +
                "   'connector' = 'filesystem', " +
                "   'path' = 'input/sensor.txt', " +
                "   'format' = 'csv' " +
                ")");
        tEnv.sqlQuery("select * from sensor").execute().print();

    }
}
