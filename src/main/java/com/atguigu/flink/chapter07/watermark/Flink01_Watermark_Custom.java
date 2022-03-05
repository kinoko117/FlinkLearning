package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/5 10:00
 */
public class Flink01_Watermark_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2000);

        env
                .socketTextStream("hadoop162", 9999)
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
                                .forGenerator(new WatermarkStrategy<WaterSensor>() {
                                    @Override
                                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                        // 返回一个水印生成器
                                        return new MyWMG();
                                    }
                                })
                                .withTimestampAssigner((element, ts) -> element.getTs())


                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
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

    public static class MyWMG implements WatermarkGenerator<WaterSensor> {

        private long maxTs = Long.MIN_VALUE + 3000 + 1;


        //每来一个元素执行一次, 更新最大时间戳
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("MyWMG.onEvent");
            maxTs = Math.max(maxTs, eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - 3000 - 1));
        }

        // 每个周期执行 默认200s
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //System.out.println("MyWMG.onPeriodicEmit");
            // 周期性水印
//            output.emitWatermark(new Watermark(maxTs - 3000 - 1));

        }
    }
}
/*
new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);

this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

每来一个元素执行一次
public void onEvent
    maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    计算最大时间戳

周期性的执行  默认 200ms
public void onPeriodicEmit
    output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
 */
