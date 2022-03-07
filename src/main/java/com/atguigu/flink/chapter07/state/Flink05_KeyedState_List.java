package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/7 9:12
 */
public class Flink05_KeyedState_List {
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

                    private ListState<Integer> top3VcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
//                        getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcState", Integer.class))
                        top3VcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {

                        top3VcState.add(value.getVc());

                        List<Integer> list = AtguiguUtil.toList(top3VcState.get());
                        /*list.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o2.compareTo(o1);
                            }
                        });*/
//                        list.sort((o1, o2) -> o2.compareTo(o1));
                        list.sort(Comparator.reverseOrder());  // 对list中的要元素进行排序

                        if (list.size() > 3) {
                            list.remove(list.size() - 1);
                        }

                        out.collect("top3: " + list);

                        // 更新top3到状态中
//                        top3VcState.clear();
                        top3VcState.update(list);  // 覆盖

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
