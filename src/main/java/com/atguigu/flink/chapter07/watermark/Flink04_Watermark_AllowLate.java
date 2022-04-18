package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/5 10:00
 */
public class Flink04_Watermark_AllowLate {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))  // 乱序不程度
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    // 返回事件时间
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                                .withIdleness(Duration.ofSeconds(10))   // 防止数据倾斜带来的水印不更新
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(2))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        List<WaterSensor> list = AtguiguUtil.toList(elements);
                        out.collect(key + "  " + ctx.window() + "  " + list);

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
/*

flink 解决乱序数据:
1. 事件时间+水印时间

2. 允许迟到
    当水印到了窗口的关闭时间, 先对窗口内的数据进行计算, 但是窗口不关闭,
     如果在允许时间内来的数据, 还可以进入这个窗口, 参与计算. 当超过这个允许时间后再斟酌的关闭窗口

     .allowedLateness(Time.seconds(2))

3. 侧输出流
    当窗口真正关闭之后, 如果仍然有迟到数据, 则把迟到放入到侧输出流中.

 */
