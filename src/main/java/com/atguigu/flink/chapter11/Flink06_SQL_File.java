package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink06_SQL_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建一个动态表与文件关联
        tEnv
                .executeSql("create table sensor(" +
                        " id string, " +
                        " ts bigint, " +
                        " vc int" +
                        ")with(" +
                        " 'connector' = 'filesystem', " +
                        " 'path' = 'input/sensor.txt', " +
                        " 'format' = 'csv' " +
                        ")");

       // tEnv.sqlQuery("select * from sensor where id='sensor_1'").execute().print();



        tEnv
                .executeSql("create table abc(" +
                        " id string, " +
                        " ts bigint, " +
                        " vc int" +
                        ")with(" +
                        " 'connector' = 'filesystem', " +
                        " 'path' = 'input/a.txt', " +
                        " 'format' = 'csv' " +
                        ")");


//        tEnv.executeSql("insert into abc select * from sensor where id='sensor_1'");
        tEnv.sqlQuery("select * from sensor where id='sensor_1'").executeInsert("abc");


    }
}
