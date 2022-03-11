package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink07_SQL_Kafka {
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
                        " 'connector' = 'kafka', " +
                        "  'topic' = 's1'," +
                        "  'properties.bootstrap.servers' = 'hadoop162:9092',\n" +
                        "  'properties.group.id' = 'atguigu',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'" +
                        ")");


        tEnv
                .executeSql("create table abc(" +
                        " id string, " +
                        " ts bigint, " +
                        " vc int" +
                        ")with(" +
                        " 'connector' = 'kafka', " +
                        "  'topic' = 's2'," +
                        "  'properties.bootstrap.servers' = 'hadoop162:9092',\n" +
                        "  'format' = 'json', " +
                        "  'sink.partitioner' = 'round-robin' " +
                        ")");


        tEnv.sqlQuery("select * from sensor").executeInsert("abc");



    }
}
