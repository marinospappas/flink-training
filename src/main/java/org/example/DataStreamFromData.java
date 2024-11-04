package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class DataStreamFromData {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Person> persons = List.of( new Person("Fred", 35), new Person("Wilma", 35), new Person("Pebbles", 2));
        DataStream<Person> flintstones = env.fromData(persons);

        DataStream<Person> adults = flintstones.filter((FilterFunction<Person>) person -> person.age >= 18);

        flintstones.printToErr();
        adults.print();
        env.execute();
    }
}