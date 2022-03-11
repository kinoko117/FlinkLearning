package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink10_Time_Event_1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(0));
        tEnv.executeSql("create table sensor(" +
                "   id string," +
                "   ts bigint, " +
                "   vc int, " +
                "   et as to_timestamp_ltz(ts, 3)," +
                "   watermark for et as et - interval '3' second " +  // ts是long型的s或者ms  0或3
                ")with(" +
                "   'connector' = 'filesystem', " +
                "   'path' = 'input/sensor.txt', " +
                "   'format' = 'csv' " +
                ")");
        Table table = tEnv.sqlQuery("select * from sensor");
        table.printSchema();
        table.execute().print();

    }
}
