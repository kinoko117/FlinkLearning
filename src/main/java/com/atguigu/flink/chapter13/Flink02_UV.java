package com.atguigu.flink.chapter13;

import com.atguigu.flink.bean.UserBehavior;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/3/4 8:56
 */
public class Flink02_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        //        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        
        
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(
                    Long.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]),
                    data[3],
                    Long.valueOf(data[4])
                );
            })
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .keyBy(UserBehavior::getBehavior)
            .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
    
                private ValueState<Long> sumState;
                private ValueState<BloomFilter<Long>> bfState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    bfState = getRuntimeContext()
                        .getState(
                            new ValueStateDescriptor<BloomFilter<Long>>(
                                "bfState",
                                TypeInformation.of(new TypeHint<BloomFilter<Long>>() {}))
                                 );
    
                    sumState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sumState", TypeInformation.of(Long.class)));
                }
                
                @Override
                public void processElement(UserBehavior ub,
                                           Context ctx,
                                           Collector<Long> out) throws Exception {
                    if (bfState.value() == null) {
                        BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), 1000 * 10000, 0.01);
                        
                        bfState.update(bf);
                        sumState.update(0L);
                        
                    }
    
                    BloomFilter<Long> bf = bfState.value();
                    if (bf.put(ub.getUserId())) {  // 表示这次添加成功
                        Long sum = sumState.value();
                        sum++;
                        sumState.update(sum);
                        
                    }
    
                    out.collect(sumState.value());
                    
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
