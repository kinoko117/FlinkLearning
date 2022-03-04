package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author lzc
 * @Date 2022/3/4 9:26
 */
public class Flink04_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 读取两个流
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env
                .readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(data[0]),
                                data[1],
                                data[2],
                                Long.valueOf(data[3])

                        );
                    }
                })
                .filter(data -> "pay".equals(data.getEventType()));

        SingleOutputStreamOperator<TxEvent> txEventStream = env.readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new TxEvent(
                                data[0],
                                data[1],
                                Long.valueOf(data[2])

                        );
                    }
                });

        orderEventStream
                .connect(txEventStream)
                .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    HashMap<String, OrderEvent> txIdToOrderEvent = new HashMap<>();
                    HashMap<String, TxEvent> txIdToTxEvent = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value,
                                                Context ctx,
                                                Collector<String> out) throws Exception {
                        String txId = value.getTxId();
                        if (txIdToTxEvent.containsKey(txId)) {
                            //对账成功
                            out.collect("订单: " + value.getOrderId() + " 对账成功!!!");
                        } else {
                            // 把订单数据存入到对应的map集合中
                            txIdToOrderEvent.put(txId, value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value,
                                                Context ctx,
                                                Collector<String> out) throws Exception {
                        String txId = value.getTxId();
                        if (txIdToOrderEvent.containsKey(txId)) {
                            out.collect("订单: " + txIdToOrderEvent.get(txId).getOrderId() + " 对账成功!!!");
                        } else {
                            txIdToTxEvent.put(txId, value);
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
