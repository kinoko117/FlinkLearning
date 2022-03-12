package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/12 11:12
 */
public class Flink04_Function_AggTable {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

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
        // 删除有空值的数据
//        tEnv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer", "drop");

        // 3. 把流转成一个表(动态表)
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        // 使用自定义函数
        // 1. 在table api中使用
        // 1.1 内联的方式使用

        // 1.2 先注册后使用


    }

    public static class Top2 extends TableAggregateFunction<Result, FirstSecond> {


        // 初始化累加器
        @Override
        public FirstSecond createAccumulator() {
            return null;
        }

        // 实现数据累加: 计算出来top2的水位, 存储到累加器中
        public void accumulate(FirstSecond fs, Integer vc){

        }

        // 方法名必须是:emitValue 参数1: 必须是累加器 参数2: Collector<Result> 泛型是你结果类型
        public void emitValue(FirstSecond fs, Collector<Result> out){}


    }

    public static class FirstSecond {
        public Integer first = 0;
        public Integer second = 0;
    }

    // 结果类型: 和需求对应, 需求有几列, 这里就应该有几个字段
    public static class Result {
        public String rank;
        public Integer vc;

        public Result(String rank, Integer vc) {
            this.rank = rank;
            this.vc = vc;
        }

        public Result() {
        }
    }


}
/*
每来一条数据, 输出水位中的top2
new WaterSensor("sensor_1", 1000L, 10),
                            名次    值
                            第一名   10
new WaterSensor("sensor_1", 2000L, 20),
                           名次    值
                           第一名   20
                           第二名   10

 new WaterSensor("sensor_1", 4000L, 40),
                           名次    值
                           第一名   40
                           第二名   20

                           ....

 */

