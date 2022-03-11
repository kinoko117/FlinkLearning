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
public class Flink11_Window_Grouped_2 {
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

        // select id, stt, edt,  from t group id, window
        /*tEnv
                .sqlQuery("select " +
                        "   id, " +
                        "   tumble_start(ts, interval '5' second) stt, " +
                        "   tumble_end(ts, interval '5' second) edt, " +
                        "   sum(vc) vc_sum " +
                        "from sensor " +
                        "group by id, tumble(ts, interval '5' second)")
                .execute()
                .print();*/


        /*tEnv
                .sqlQuery("select " +
                        "   id, " +
                        "   hop_start(ts, interval '2' second,interval '5' second) stt, " +
                        "   hop_end(ts, interval '2' second,interval '5' second) edt, " +
                        "   sum(vc) vc_sum " +
                        "from sensor " +
                        "group by id, hop(ts, interval '2' second,interval '5' second)")
                .execute()
                .print();*/

        tEnv
                .sqlQuery("select " +
                        "   id, " +
                        "   session_start(ts, interval '2' second) stt, " +
                        "   session_end(ts, interval '2' second) edt, " +
                        "   sum(vc) vc_sum " +
                        "from sensor " +
                        "group by id, session(ts, interval '2' second)")
                .execute()
                .print();




    }
}
/*
grouped窗口
keyBy: id  -> window: 0-05 5-10

select id, stt, edt,  from t group id, window


over窗口



 */