package com.cherry.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

public class FlinkRunnerDemo {
    public static void main(String[] args) {
        // 创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定 PipelineRunner：FlinkRunner 必须指定如果不制定则为本地
        options.setRunner(FlinkRunner.class);
        // 设置相关管道
        Pipeline pipeline = Pipeline.create(options);
        PCollection<KafkaRecord<String, String>> lines =
                pipeline.apply(KafkaIO.<String, String>read().withBootstrapServers("192.168.1.14:9092")
                        .withTopic("testmsg")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .updateConsumerProperties(ImmutableMap.<String, Object>of("auto.offset.reset", "earliest")));
         // 为输出的消息类型。或者进行处理后返回的消息类型
        PCollection<String> kafkadata = lines.apply("Remove Kafka Metadata", ParDo.of(new DoFn<KafkaRecord<String, String>, String>() {
            private static final long serialVersionUID = 1L;

            @ProcessElement
            public void processElement(ProcessContext ctx) {
                System.out.print("输出的分区为 ----：" + ctx.element().getKV());
                ctx.output(ctx.element().getKV().getValue());
            }
        }));
        PCollection<String> windowedEvents = kafkadata.apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))));
        PCollection<KV<String, Long>> wordcount = windowedEvents.apply(Count.<String>perElement()); // 统计每一个 kafka 消息的 Count
        PCollection<String> wordtj = wordcount.apply("ConcatResultKVs", MapElements.via( // 拼接最后的格式化输出（Key 为 Word，Value 为 Count）
                new SimpleFunction<KV<String, Long>, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String apply(KV<String, Long> input) {
                        System.out.print("进行统计：" + input.getKey() + ": " + input.getValue());
                        return input.getKey() + ": " + input.getValue();
                    }
                }));
        wordtj.apply(KafkaIO.<Void, String>write().withBootstrapServers("192.168.1.14:9092")// 设置写会 kafka 的集群配置地址
                .withTopic("senkafkamsg")// 设置返回 kafka 的消息主题
                // .withKeySerializer(StringSerializer.class)// 这里不用设置了，因为上面 Void
                .withValueSerializer(StringSerializer.class)
                .values() // 只需要在此写入默认的 key 就行了，默认为 null 值
        ); // 输出结果
        pipeline.run().waitUntilFinish();

    }
}
