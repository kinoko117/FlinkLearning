package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/7 9:12
 */
public class Flink07_KeyedState_Aggregate {
    public static void main(String[] args) {
        /*
        从socket读数据, 存入到一个ArrayList
        如何把ArrayList中的数据存入到列表状态, 并在程序恢复能够从状态把数据再恢复到ArrayList
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

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
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {


                    private AggregatingState<WaterSensor, Double> avgVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgVcState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<WaterSensor, Avg, Double>(
                                        "avgVcState",
                                        new AggregateFunction<WaterSensor, Avg, Double>() {
                                            @Override
                                            public Avg createAccumulator() {
                                                return new Avg();
                                            }

                                            @Override
                                            public Avg add(WaterSensor value, Avg acc) {
                                                acc.sum += value.getVc();
                                                acc.count++;
                                                return acc;
                                            }

                                            @Override
                                            public Double getResult(Avg acc) {
                                                return acc.avg();

                                            }

                                            @Override
                                            public Avg merge(Avg a, Avg b) {
                                                System.out.println("Flink07_KeyedState_Aggregate.merge");
                                                a.sum += b.sum;
                                                a.count += b.count;
                                                return a;
                                            }
                                        },
                                        Avg.class
                                ));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        avgVcState.add(value);


                        Double avg = avgVcState.get();
                        out.collect(ctx.getCurrentKey() + " 的平均水位: " + avg);

                    }
                })
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Avg {
        public Integer sum = 0;
        public Long count = 0L;

        public Double avg() {
            return sum * 1.0 / count;
        }
    }


}
