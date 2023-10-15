package com.klimovich.kafkastream.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BodyTemp {
    private String timestamp;
    private Double temp;
    private String unit;

    @Override
    public String toString() {
        return "BodyTemp{" +
                "timestamp='" + timestamp + '\'' +
                ", temp=" + temp +
                ", unit='" + unit + '\'' +
                '}';
    }
}
