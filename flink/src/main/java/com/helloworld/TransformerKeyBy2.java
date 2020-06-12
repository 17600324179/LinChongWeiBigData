package com.helloworld;

/**
 * @Description:
 * @Author: chongweiLin
 * @CreateDate: 2020-06-12 15:04
 **/
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by www.xueai8.com
 */

public class TransformerKeyBy2 {

    // POJO类，用来表示事件流中的事件
    public static class Person {
        public String name;        // 姓名
        public String gender;      // 性别

        public Person() {}

        public Person(String name, String gender) {
            this.name = name;
            this.gender = gender;
        }

        @Override
        public String toString() {
            return this.name + ": 性别 " + this.gender;
        }
    }

    public static void main(String[] args) throws Exception {
        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 加载数据源
        DataStream<Person> personDS = env.fromElements(
                new Person("张三", "男"),
                new Person("李四", "女"),
                new Person("小多米", "男"));

        // 执行keyBy转换，按性别(gender)分区
        personDS.keyBy("gender").print();

        // 执行
        env.execute("flink keyBy transformatiion");
    }
}