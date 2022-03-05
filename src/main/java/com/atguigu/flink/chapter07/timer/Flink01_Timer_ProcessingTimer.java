package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/5 14:59
 */
public class Flink01_Timer_ProcessingTimer {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop162", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                });

        stream
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {

                        if (value.getVc() > 20) {
                            long ts = System.currentTimeMillis() + 5000;  // 5s后

                            ctx.timerService().registerProcessingTimeTimer(ts);  // 注册一个基于处理时间的5s触发的定时器
                        }

                        // 如果水位小于10 取消掉定时器


                    }

                    // 当定时器触发的时候, 执行这个方法
                    // 参数1: 正在触发的定时器的时间
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 传感器检测到水位超过20, 红色预警....");
                    }
                })
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
