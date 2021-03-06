package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/8 9:55
 * <p>
 * private Long userId;
 * private String ip;
 * private String eventType;
 * private Long eventTime;
 */
public class Flink06_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        /*
            private Long orderId;
            private String eventType;
            private String txId;
            private Long eventTime;
         */
        env
                .readTextFile("input/OrderLog.csv")
                .map(value -> {
                    String[] data = value.split(",");
                    return new OrderEvent(
                            Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.valueOf(data[3]) * 1000

                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, ts) -> event.getEventTime())
                )
                .keyBy(OrderEvent::getOrderId)
                .window(EventTimeSessionWindows.withGap(Time.minutes(15L)))
                .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {

                    private ValueState<OrderEvent> createState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                    }

                    @Override
                    public void process(Long orderId,
                                        Context ctx,
                                        Iterable<OrderEvent> elements,
                                        Collector<String> out) throws Exception {
                        List<OrderEvent> list = AtguiguUtil.toList(elements);
                        if (list.size() == 2) {  // ?????????????????????: create???pay??????, ????????????
                            out.collect("??????: " + orderId + " ????????????...");
                        }else{
                            // ??????????????????, ??????????????????
                            OrderEvent orderEvent = list.get(0);
                            String eventType = orderEvent.getEventType();

                            if ("create".equals(eventType)) {
                                // ??????create??????, ???create??????????????????
                                createState.update(orderEvent);
                            }else{  // ???????????????????????????pay
                                if (createState.value() != null) {
                                    // ??????pay???, ?????????????????????45??????
                                    out.collect("??????: " + orderId + " ????????????, ?????????????????????...");
                                }else{
                                    // pay??????, ??????create????????????
                                    out.collect("??????: " + orderId + " ????????????????????????, ?????????????????????...");
                                }
                            }
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
