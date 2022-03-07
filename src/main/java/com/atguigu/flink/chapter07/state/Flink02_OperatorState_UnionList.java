package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/7 9:12
 */
public class Flink02_OperatorState_UnionList {
    public static void main(String[] args) {
        /*
        从socket读数据, 存入到一个ArrayList
        如何把ArrayList中的数据存入到列表状态, 并在程序恢复能够从状态把数据再恢复到ArrayList
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 开启checkpoint
        env.enableCheckpointing(3000);

        env
                .socketTextStream("hadoop162", 9999)
                .flatMap(new MyFlatMapFunction())
                .print();



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, String>, CheckpointedFunction{
        List<String> words = new ArrayList<String>();
        private ListState<String> listState;

        @Override
        public void flatMap(String line, Collector<String> out) throws Exception {

            if (line.contains("x")) {
                throw new RuntimeException("抛异常, 程序自动重启....");
            }

            for (String word : line.split(" ")) {
                words.add(word);
            }
            out.collect(words.toString());

        }

        // 初始化状态
        // 从状态中恢复数据. 当程序启动的时候执行.
        // 执行的次数和并行度一致
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            System.out.println("MyFlatMapFunction.initializeState");
            // 获取一个联合列表状态
            listState = ctx.getOperatorStateStore().getUnionListState(new ListStateDescriptor<String>("listState", String.class));
            // 从列表状态中取值, 恢复树到ArrayList中
            for (String word : listState.get()) {
                words.add(word);
            }
        }
        // 对状态做快照, 周期性的执行, 把状态进行保存
        // 执行的次数和并行度一致
        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
//            System.out.println("MyFlatMapFunction.snapshotState");
            // 把ArrayList中的数据保存到列表中
            /*listState.clear();
            listState.addAll(words);
*/
            listState.update(words);

        }


    }

}
