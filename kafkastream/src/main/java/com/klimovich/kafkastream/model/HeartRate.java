package com.klimovich.kafkastream.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class HeartRate {
    private String timestamp;
    private Integer pulsePerMinute;

    @Override
    public String toString() {
        return "HeartRate{" +
                "timestamp='" + timestamp + '\'' +
                ", pulsePerMinute=" + pulsePerMinute +
                '}';
    }
}
