package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/3/11 9:02
 */
public class Flink01_Table_BaseUse {
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
        // 3. 在表上执行查询(连续查询)
        // select id,vc, ts from t;
        /*Table result = table.select("*");
        // 4. 把结果表转成流, 输出
        DataStream<WaterSensor> resultStream = tEnv.toAppendStream(result, WaterSensor.class);// 表中的数据只有新增, 没有删除和更新*/

//        Table result = table.select("id, vc");
//        Table result = table.select(Expressions.$("id"), Expressions.$("vc"));
//        Table result = table.select($("id"), $("vc"));
        Table result = table
                .where($("id").isEqual("sensor_1"))
                .select($("id").as("a"), $("vc"));
        result.printSchema();  // 打印表原数据
        DataStream<Row> resultStream = tEnv.toAppendStream(result, Row.class);
        resultStream.print();
        env.execute();
    }
}
