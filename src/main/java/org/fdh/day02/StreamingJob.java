/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fdh.day02;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 访问http://localhost:8082 可以看到Flink WebUI
        //本地idea开发工具调试方便，部署到集群中不需要
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8082);
//        // lamda方式
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
//        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5, 6);
//        SingleOutputStreamOperator<String> lamdaDataStream = dataStream.map(input -> "lambda input:" + input + ",output:" + (input * 2));
//        lamdaDataStream.print();
//
//        SingleOutputStreamOperator<String> functionDataStream = dataStream.map(new DoubleMapFunction());
//        functionDataStream.print();
//        SingleOutputStreamOperator<Integer> filter = dataStream.filter(input -> input >= 4);
//        filter.print();
//        env.execute("lambda test");

        //key by
//        env.fromElements(new Word("flink", 1)
//                        , new Word("hello", 2)
//                        , new Word("flink", 2)
//                        , new Word("hello", 4))
//                //使用数字位置和名字方法已经废弃
//                .keyBy(in -> in.wordName)
//                .sum("wordCount")
//                .print();
//        env.execute("keyBy");

//        其实，这些聚合函数里已经使用了状态数据，比如，sum()内部记录了当前的和，max()内部记录了当前的最大值。
//        聚合函数的计算过程其实就是不断更新状态数据的过程。由于内部使用了状态数据，而且状态数据并不会被清除，
//        因此一定要慎重地在一个无限数据流上使用这些聚合函数。
//        env.fromElements(Tuple3.of(1, 0, 1)
//                        , Tuple3.of(1, 1, 2), Tuple3.of(1, 4, 3)
//                        , Tuple3.of(1, 3, 4), Tuple3.of(1, 9, 5))
//                .keyBy(input -> input.f0)
//                //maxby保留了其他字段最大时候的值
//                .maxBy(1)
//                .print();
//
//        env.execute("max");
        //4、reduce
//        env.fromElements(Score.of("Li", "English", 90),
//                        Score.of("Wang", "English", 88),
//                        Score.of("Li", "Math", 85),
//                        Score.of("Wang", "Math", 92),
//                        Score.of("Liu", "Math", 91),
//                        Score.of("Liu", "English", 87))
//                .keyBy("name")
////                .reduce(new MyReduceFunction())
//                .reduce((input1, input2) -> Score.of(input1.name, "Sum", input1.score + input2.score))
//                //对reduce算子单独设置并行度
//                .setParallelism(2)
//                .print();
//        env.execute("reduce");
        DataStreamSource<String> dataStream = env.fromElements("hello flink", "test me");
        dataStream.flatMap((String input, Collector<String> collector) -> {
                    for (String word : input.split(" ")) {
                        collector.collect(word);
                    }
                })
                .returns(Types.STRING)
                .print();
        env.execute("ttt");
        // 提供类型信息以解决类型擦除问题
//                .returns(Types.STRING);
    }

    public static class MyReduceFunction implements ReduceFunction<Score> {
        @Override
        public Score reduce(Score s1, Score s2) {
            return Score.of(s1.name, "Sum", s1.score + s2.score);
        }
    }

    public static class Score {
        public String name;
        public String course;
        public int score;

        public Score() {
        }

        public Score(String name, String course, int score) {
            this.name = name;
            this.course = course;
            this.score = score;
        }

        public static Score of(String name, String course, int score) {
            return new Score(name, course, score);
        }

        @Override
        public String toString() {
            return "(" + this.name + ", " + this.course + ", " + Integer.toString(this.score) + ")";
        }
    }
}
