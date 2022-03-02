package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/3/2 13:56
 */
public class Flink03_Sink_Es {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                });


        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("hadoop162", 9200));
        hosts.add(new HttpHost("hadoop163", 9200));
        hosts.add(new HttpHost("hadoop164", 9200));

        ElasticsearchSink.Builder<WaterSensor> builder = new ElasticsearchSink.Builder<>(
                hosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element,  // 要写入的元素
                                        RuntimeContext ctx, // 上下文对象
                                        RequestIndexer indexer) {  // 要写入的index对象, 存入到RequestIndexer
                        String data = JSON.toJSONString(element);

                        IndexRequest indexRequest = Requests.indexRequest()
                                .index("sensor")  // index
                                .type("doc")  // 不能以下划线开头  _doc 这个是唯一可以使用下划线
                                .id(element.getId())
                                .source(data, XContentType.JSON);
                        indexer.add(indexRequest);

                    }
                });

        builder.setBulkFlushInterval(2000); // 2s刷新一次
        builder.setBulkFlushMaxActions(3);
        builder.setBulkFlushMaxSizeMb(1);


        stream
                .keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(builder.build());


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
index     数据库
type      表
    <6.x
        一个index可以有多个type
    6.x< x < 7.x
        一个index只能有一个type
    > 7.x
        把type去掉了
document  行  每行一个id
    field...



 */