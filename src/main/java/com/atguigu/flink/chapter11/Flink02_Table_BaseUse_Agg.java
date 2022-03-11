package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink02_Table_BaseUse_Agg {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 先创建一个流
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //2. 需要现有相应的表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 把流转成一个表(动态表)
        Table table = tEnv.fromDataStream(stream);

        // select id, sum(vc) vc_sum from t group by id
        Table result = table
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"), $("vc_sum"));
        // 当有删除或者更新的时候, 使用撤回流
        /*DataStream<Tuple2<Boolean, Row>> resultStream = tEnv.toRetractStream(result, Row.class);
        resultStream
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .print();*/

        result.execute().print();  // idea端调试sql代码使用


//        env.execute();
    }
}
