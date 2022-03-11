package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink11_Window_Grouped_1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 先创建一个流
        DataStream<WaterSensor> stream = env
                .fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4001L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ws, ts) -> ws.getTs())

                );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table table = tEnv.fromDataStream(stream, $("id"), $("ts").rowtime(), $("vc"));

        // 1. 在table api中使用窗口 over(窗口长度)
//        GroupWindow w = Tumble.over(lit(5).seconds()).on($("ts")).as("win");
//        GroupWindow w = Slide.over(lit(5).seconds()).every(lit(2).second()).on($("ts")).as("win");
        GroupWindow w = Session.withGap(lit(2).second()).on($("ts")).as("win");

        table
                .window(w)
                .groupBy($("id"), $("win"))
                .select($("id"),
                        $("win").start().as("stt"),
                        $("win").end().as("edt"),
                        $("vc").sum().as("vc_sum")
                )
                .execute()
                .print();


        // 2. 在sql语句中使用窗口


    }
}
/*
grouped窗口
keyBy: id  -> window: 0-05 5-10

select id, stt, edt,  from t group id, window


over窗口



 */