package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/3/8 9:55
 */
public class Flink04_Ads {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .readTextFile("input/AdClickLOg.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new AdsClickLog(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            data[2],
                            data[3],
                            Long.valueOf(data[4]) * 1000
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((log, ts) -> log.getTimestamp())
                )
                // 每个用户对每个广告的点击
                .keyBy(log -> log.getUserId() + "_" + log.getAdsId())
                .process(new KeyedProcessFunction<String, AdsClickLog, String>() {

                    private ValueState<String> yesterdayState;
                    private ValueState<Boolean> blackListState;
                    private ReducingState<Long> clickCountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        clickCountState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>("clickCountState", Long::sum, Long.class));
                        blackListState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blackListState", Boolean.class));
                        yesterdayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("yesterdayState", String.class));
                    }

                    @Override
                    public void processElement(AdsClickLog value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        // 一旦跨天, 状态应该重置
                        // 如何判断数据跨天?  判断年月日是否变化
                        String yesterday = yesterdayState.value();
                        String today = new SimpleDateFormat("yyyy-MM-dd").format(value.getTimestamp());
                        if (!today.equals(yesterday)) {  // 表示数据进入到第二天
                            yesterdayState.update(today);  // 把今天的日期存入到状态, 明天就可以读到今天是昨天

                            clickCountState.clear();
                            blackListState.clear();

                        }


                        // 如果已经进入了黑名单, 则不需要再进行统计
                        if (blackListState.value() == null) {
                            clickCountState.add(1L);
                        }
                        Long count = clickCountState.get();
                        String msg = "用户: " + value.getUserId() + "对广告:" + value.getAdsId() + "的点击量是: " + count;

                        if (count > 99) {
                            // 第一次超过99加入黑名单, 以后就不用再加
                            if (blackListState.value() == null) {
                                msg += " 超过阈值 99, 加入黑名单";
                                out.collect(msg);
                                blackListState.update(true);
                            }
                        } else {
                            out.collect(msg);

                        }

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
