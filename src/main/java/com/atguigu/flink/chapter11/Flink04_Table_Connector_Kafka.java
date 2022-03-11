package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink04_Table_Connector_Kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tEnv
                .connect(
                        new Kafka()
                                .version("universal")
                                .property("bootstrap.servers", "hadoop162:9092")
                                .property("group.id", "atguigu")
                                .topic("s1")
                                .startFromLatest()
                )
//                .withFormat(new Csv())
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // tEnv.from("sensor").execute().print();

        Table result = tEnv.from("sensor").select($("id"), $("vc"));

        // 动态与输出的topic进行关联
        tEnv
                .connect(
                        new Kafka()
                                .version("universal")
                                .property("bootstrap.servers", "hadoop162:9092")
                                .topic("s2")
                                .sinkPartitionerRoundRobin()
                )
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("vc", DataTypes.INT()))
                .createTemporaryTable("out");

        result.executeInsert("out");

    }
}
