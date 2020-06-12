package com.helloworld;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description:
 * @Author: chongweiLin
 * @CreateDate: 2020-06-12 12:00
 **/
public class FlinkJavaWordCount {
    /**
     * 主要为了存储单词以及单词出现的次数
     */
    @Data
    public static class WordWithCount {
        public String word;
        public long count;


        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }
    }

    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStreamSource<String> text = env.readTextFile("hdfs://192.168.1.101:9000/flink/data/t_kt_payflow_20200610-20200611.txt");

        SingleOutputStreamOperator<WordWithCount> mapStream = text.map(new MapFunction<String, WordWithCount>() {
            @Override
            public WordWithCount map(String value) throws Exception {
                return new WordWithCount(value.split("\t")[0], 1L);
            }
        });

        mapStream.keyBy("word").sum("count").setParallelism(1);

//        mapStream.keyBy(new KeySelector<Object, String>() {
//            @Override
//            public String getKey(Object value) throws Exception {
//                return ((WordWithCount)(value)).getWord();
//            }
//        }).sum("count")
//                .print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

    }


}
