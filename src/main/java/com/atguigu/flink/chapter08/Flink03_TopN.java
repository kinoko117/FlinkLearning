package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.HotItem;
import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/8 9:55
 */
public class Flink03_TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4]) * 1000);
                })
                .filter(ub -> "pv".equals(ub.getBehavior()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ub, ts) -> ub.getTimestamp())

                )
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(2), Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(UserBehavior value, Long acc) {
                                return acc + 1;
                            }

                            @Override
                            public Long getResult(Long acc) {
                                return acc;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        },
                        new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                            @Override
                            public void process(Long itemId,
                                                Context ctx,
                                                Iterable<Long> elements,
                                                Collector<HotItem> out) throws Exception {

                                Long count = elements.iterator().next();
                                long wEnd = ctx.window().getEnd();
                                out.collect(new HotItem(itemId, wEnd, count));

                            }
                        })
                .keyBy(HotItem::getWEnd)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ListState<HotItem> hotItemState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotItemState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                    }

                    @Override
                    public void processElement(HotItem value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        // 结束是1这个窗口的第一条数据进来的时候, 注册定时器
                        // 如果list状态中没有元素集市第一个,
                        if (!hotItemState.get().iterator().hasNext()) {
                            // 注册定时器
                            ctx.timerService().registerEventTimeTimer(value.getWEnd() + 1000);
                        }

                        hotItemState.add(value);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 当定时器触发的时候, 证明同一个窗口内的点击量数据已经来齐了, 这个时候可以排序取topN
                        List<HotItem> hotItems = AtguiguUtil.toList(hotItemState.get());

                        // 排序
                        hotItems.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

                        String msg = "-------------------\n";
                        for (int i = 0, len = Math.min(3, hotItems.size()); i < len; i++) {
                            msg += hotItems.get(i) + "\n";
                        }

                        out.collect(msg);

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
