package com.atguigu.flink.chapter10;

import com.atguigu.flink.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


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
        KeyedStream<OrderEvent, Long> stream = env
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
                .keyBy(OrderEvent::getOrderId);

        // create + pay
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                }).optional()
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(45));

        // 假设pay来晚了, 或者没有pay, 则create金融局超时数据
        PatternStream<OrderEvent> ps = CEP.pattern(stream, pattern);

        /*SingleOutputStreamOperator<String> normal = ps
                .select(
                        new OutputTag<String>("timeout") {
                        },
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> pattern,
                                                  long timeoutTimestamp) throws Exception {
                                OrderEvent create = pattern.get("create").get(0);
                                return "订单:" + create.getOrderId() + " 没有pay或者pay超时支付...";
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> map) throws Exception {
                                // create还optional, 则map有两种情况: 只有pay  或者既有 create 又有 pay
                                return map.toString();
                            }
                        }

                );*/
        SingleOutputStreamOperator<String> normal = ps.flatSelect(
                new OutputTag<String>("timeout") {
                },
                new PatternFlatTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public void timeout(Map<String, List<OrderEvent>> pattern,
                                        long timeoutTimestamp,
                                        Collector<String> out) throws Exception {
                        OrderEvent create = pattern.get("create").get(0);
                        String msg = "订单:" + create.getOrderId() + " 没有pay或者pay超时支付...";
                        out.collect(msg);

                    }
                },
                new PatternFlatSelectFunction<OrderEvent, String>() {

                    @Override
                    public void flatSelect(Map<String, List<OrderEvent>> map,
                                           Collector<String> out) throws Exception {
                        // 既有create又有pay是正常的, 放弃
                        // 只取只有pay
                        if (!map.containsKey("create")) {
                            OrderEvent pay = map.get("pay").get(0);
                            String msg = "订单:" + pay.getOrderId() + " 只有pay没有create, 请检查系统bug";
                            out.collect(msg);
                        }
                    }
                }
        );

        normal.print("正常");
        normal.getSideOutput(new OutputTag<String>("timeout") {
        }).print("异常");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
