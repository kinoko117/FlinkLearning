package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink14_Hive {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 创建hivecatalog   参数3: hive的配置文件所在的目录
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "gmall", "input/");

        // 2. 注册hiveCatalog
        tEnv.registerCatalog("hive", hiveCatalog);
        tEnv.useCatalog("hive");
        tEnv.useDatabase("gmall");

        // 3. 使用, 读取数据
        /*tEnv.sqlQuery("select " +
                " * " +
                "from hive.gmall.person").execute().print();*/

        tEnv.sqlQuery("select " +
                " * " +
                "from person").execute().print();


    }
}
