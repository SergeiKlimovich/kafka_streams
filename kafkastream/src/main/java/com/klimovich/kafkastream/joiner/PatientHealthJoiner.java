package com.klimovich.kafkastream.joiner;

import org.apache.kafka.streams.kstream.ValueJoiner;

import com.klimovich.kafkastream.model.BodyTemp;
import com.klimovich.kafkastream.model.HeartRate;
import com.klimovich.kafkastream.model.PatientHealth;

public class PatientHealthJoiner implements ValueJoiner<HeartRate, BodyTemp, PatientHealth> {

    @Override
    public PatientHealth apply(HeartRate heartRate, BodyTemp bodyTemp) {

        return PatientHealth.PatientHealthBuilder.aPatientVitals().
                withHeartRate(heartRate.getPulsePerMinute())
                .withTemp(bodyTemp.getTemp())
                .withUnit(bodyTemp.getUnit())
                .build();
    }
}
