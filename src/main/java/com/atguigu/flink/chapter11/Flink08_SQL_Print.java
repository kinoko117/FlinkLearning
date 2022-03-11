package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink08_SQL_Print {
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

        //tEnv.sqlQuery("select * from sensor").execute().print();


        tEnv
                .executeSql("create table abc(" +
                        " id string, " +
                        " ts bigint, " +
                        " vc int" +
                        ")with(" +
                        " 'connector' = 'print' " +
                        ")");

        tEnv
                .executeSql("create table def(" +
                        " id string, " +
                        " vc int" +
                        ")with(" +
                        " 'connector' = 'print' " +  // 方便在平台上进行调试
                        ")");

        tEnv.sqlQuery("select * from sensor").executeInsert("abc");
        tEnv.sqlQuery("select id, sum(vc) vc from sensor group by id").executeInsert("def");


    }
}
