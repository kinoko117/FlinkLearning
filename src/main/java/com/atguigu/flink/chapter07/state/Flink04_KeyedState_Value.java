package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/7 9:12
 */
public class Flink04_KeyedState_Value {
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
                .flatMap(new RichFlatMapFunction<WaterSensor, String>() {

                    private ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // alt+ctrl+f
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class));
                    }

                    @Override
                    public void flatMap(WaterSensor value,
                                        Collector<String> out) throws Exception {
                        // 如果是第一条数据, 则把水位存到状态中, 如果不是第一条, 则把状态中的水位值取出
                        if (lastVcState.value() != null) {
                            // 不是第一条数据
                            Integer lastVc = lastVcState.value();
                            if (lastVc > 10 && value.getVc() > 10) {
                                out.collect(value.getId() + " 连续两次水位超过10, 发出橙色预警...");

                            }
                        }
                        lastVcState.update(value.getVc());

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
