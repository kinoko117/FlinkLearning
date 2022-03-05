package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/3/5 14:59
 */
public class Flink03_Timer_Practice {
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

                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        stream
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private long ts;
                    boolean isFirst = true;
                    int lastVc;

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        /*
                        当第一条数据来的时候, 注册一个定时器5s后触发的定时器
                        后面的数据如果是上升, 则什么都不坐
                        如果下降, 则取消定时器, 顺便注册一个新的定时器5s后触发的定时器
                         */
                        if (isFirst) {
                            isFirst = false;
                            // 注册定时器
                            ts = value.getTs() + 5000;
                            System.out.println("第一条数据过来: 注册定时器 " + ts);
                            ctx.timerService().registerEventTimeTimer(ts);


                        } else {
                            // 不是第一条, 判断这次的水位相比上次是否上次, 如果上升则什么都不坐, 如果没有上升, 则删除定时器
                            if (value.getVc() < lastVc) {
                                System.out.println("水位下降: 取消定时器 " + ts + ", 水位:   " + lastVc + "  " + value.getVc());
                                ctx.timerService().deleteEventTimeTimer(ts);
                                // 重新注册新的定时器
                                ts = value.getTs() + 5000;
                                System.out.println("第一条数据过来: 注册定时器 " + ts);
                                ctx.timerService().registerEventTimeTimer(ts);
                            } else {
                                System.out.println("水位上升什么都不做: " + ", 水位:   " + lastVc + "  " + value.getVc());
                            }
                        }

                        lastVc = value.getVc();
                    }

                    // 当定时器触发的时候, 执行这个方法
                    // 参数1: 正在触发的定时器的时间
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 水位5s内连续上升, 红色预警");
                        isFirst = true;
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
