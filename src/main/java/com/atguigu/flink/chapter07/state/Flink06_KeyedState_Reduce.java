package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/7 9:12
 */
public class Flink06_KeyedState_Reduce {
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


                    private ReducingState<Integer> sumVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        /*sumVcState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "sumVcState",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                        return value1 + value2;
                                    }
                                },
                                Integer.class
                        ));*/
                       /* sumVcState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "sumVcState",
                                (value1, value2) -> value1 + value2,
                                Integer.class
                        ));*/
                        sumVcState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "sumVcState",
                                Integer::sum,
                                Integer.class
                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        sumVcState.add(value.getVc());


                        out.collect(ctx.getCurrentKey() + " 的水位和: " + sumVcState.get());
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
