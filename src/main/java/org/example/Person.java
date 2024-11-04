package org.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {

    public String name;
    public Integer age;

    public String toString() {
        return "Person: " + this.name + ": age " + this.age;
    }
}
