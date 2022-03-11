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
public class Flink12_Window_TVF_1 {
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
        tEnv.createTemporaryView("sensor", table);


        /*tEnv
                .sqlQuery("select" +
                        " id, window_start, window_end, " +
                        " sum(vc) vc_sum " +
                        "from table(tumble( table sensor, descriptor(ts), interval '5' second  ))" +
                        "group by id, window_start, window_end")
                .execute()
                .print();*/

         /*tEnv
                .sqlQuery("select" +
                        " id, window_start, window_end, " +
                        " sum(vc) vc_sum " +
                        "from table(tumble(DATA => table sensor,TIMECOL => descriptor(ts), SIZE =>interval '5' second  ))" +
                        "group by id, window_start, window_end")
                .execute()
                .print();*/

         // 滑动窗口的长度必须是滑动步长的整数倍
        tEnv
                .sqlQuery("select" +
                        " id, window_start, window_end, " +
                        " sum(vc) vc_sum " +
                        "from table(hop(table sensor, descriptor(ts), interval '2' second, interval '4' second  ))" +
                        "group by id, window_start, window_end")
                .execute()
                .print();


    }
}
