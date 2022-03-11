package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink12_Window_TVF_3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 先创建一个流
        DataStream<WaterSensor> stream = env
                .fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_2", 4001L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_1", 6000L, 60),
                        new WaterSensor("sensor_1", 8000L, 80),
                        new WaterSensor("sensor_1", 11000L, 110),
                        new WaterSensor("sensor_1", 13000L, 130),
                        new WaterSensor("sensor_1", 18000L, 180),
                        new WaterSensor("sensor_1", 21000L, 210)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ws, ts) -> ws.getTs())

                );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table table = tEnv.fromDataStream(stream, $("id"), $("ts").rowtime(), $("vc"));
        tEnv.createTemporaryView("sensor", table);


        // 累积窗口
        /*tEnv
                .sqlQuery("select" +
                        " 'a' id, window_start, window_end, " +
                        " sum(vc) vc_sum " +
                        "from table( tumble(table sensor, descriptor(ts), interval '5' second) )" +
                        "group by window_start, window_end " +
                        "union " +
                        "select" +
                        " id, window_start, window_end, " +
                        " sum(vc) vc_sum " +
                        "from table( tumble(table sensor, descriptor(ts), interval '5' second) )" +
                        "group by id, window_start, window_end " +
                        "")
                .execute()
                .print();*/
        tEnv
                .sqlQuery("select" +
                        " id, window_start, window_end, " +
                        " sum(vc) vc_sum " +
                        "from table( tumble(table sensor, descriptor(ts), interval '5' second) )" +
                        "group by window_start, window_end, grouping sets( (id), () )" +
                        "")
                .execute()
                .print();


    }
}
/*
分组集  group sets

------

+----+--------------------------------+-------------------------+-------------------------+-------------+
| op |                             id |            window_start |              window_end |      vc_sum |
+----+--------------------------------+-------------------------+-------------------------+-------------+
| +I |                       sensor_1 | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |         100 |
| +I |                       sensor_1 | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |         190 |
| +I |                       sensor_1 | 1970-01-01 00:00:10.000 | 1970-01-01 00:00:15.000 |         240 |
| +I |                       sensor_1 | 1970-01-01 00:00:15.000 | 1970-01-01 00:00:20.000 |         180 |
| +I |                       sensor_1 | 1970-01-01 00:00:20.000 | 1970-01-01 00:00:25.000 |         210 |

-----
+----+-------------------------+-------------------------+-------------+
| op |            window_start |              window_end |      vc_sum |
+----+-------------------------+-------------------------+-------------+
| +I | 1970-01-01 00:00:00.000 | 1970-01-01 00:00:05.000 |         100 |
| +I | 1970-01-01 00:00:05.000 | 1970-01-01 00:00:10.000 |         190 |
| +I | 1970-01-01 00:00:10.000 | 1970-01-01 00:00:15.000 |         240 |
| +I | 1970-01-01 00:00:15.000 | 1970-01-01 00:00:20.000 |         180 |
| +I | 1970-01-01 00:00:20.000 | 1970-01-01 00:00:25.000 |         210 |
+----+-------------------------+-------------------------+-------------+



 */