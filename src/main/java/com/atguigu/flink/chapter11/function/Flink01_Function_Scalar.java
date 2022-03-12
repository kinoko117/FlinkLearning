package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author lzc
 * @Date 2022/3/12 11:12
 */
public class Flink01_Function_Scalar {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 把流转成一个表(动态表)
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);


        // 使用自定义函数
        // 1. 在table api中使用
        // 1.1 内联的方式使用    call(MyToUpperCase.class, $("id")) 调用自定义函数  参数2: 传给函数的参数   upper($('id'))
        /*table
                .select($("id"), call(MyToUpperCase.class, $("id")).as("my_upper"))
                .execute()
                .print();
*/

        // 1.2 先注册后使用
        /*tEnv.createTemporaryFunction("my_upper", MyToUpperCase.class);
        table
                .select($("id"), call("my_upper", $("id")).as("my_upper"))
                .execute()
                .print();*/
        // 2. 在sql中使用
        // 先注册
        tEnv.createTemporaryFunction("my_upper", MyToUpperCase.class);
        tEnv.sqlQuery("select id, my_upper(id) id1 from sensor").execute().print();


    }

    public static class MyToUpperCase extends ScalarFunction {
        // 方法名必须是eval 这是一种约定
        // 参数和返回值 可以根据自己的情况来定
        public String eval(String s){
            return s == null ? null : s.toUpperCase();
        }
    }
}
