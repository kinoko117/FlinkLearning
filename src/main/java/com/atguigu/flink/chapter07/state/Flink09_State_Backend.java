package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/7 9:12
 */
public class Flink09_State_Backend {
    public static void main(String[] args) throws IOException {
        //shift + ctrl + u
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        /*
        从socket读数据, 存入到一个ArrayList
        如何把ArrayList中的数据存入到列表状态, 并在程序恢复能够从状态把数据再恢复到ArrayList
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(2000);  // 开启checkpoint

        // 1. 内存
        // 1.1 旧
//        env.setStateBackend(new MemoryStateBackend());   // 默认配置
        //1.2 新
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // 2. fs
        //2.1 旧
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/ck"));
        // 2.2 新
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck1");
        // 3. rocksDb
        // 3.1 旧
//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/ck2"));
        // 3.2 新
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck3");


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


                    private MapState<Integer, Object> vcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Integer, Object>("vcState", Integer.class, Object.class));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        vcState.put(value.getVc(), new Object());


                        List<Integer> vcs = AtguiguUtil.toList(vcState.keys());
                        out.collect(ctx.getCurrentKey() + " 不重复的水位值:" + vcs);
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
