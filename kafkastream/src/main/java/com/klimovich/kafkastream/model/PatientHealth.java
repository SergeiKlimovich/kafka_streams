package com.klimovich.kafkastream.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PatientHealth {
    private int heartRate;
    private Double temp;
    private String unit;

    @Override
    public String toString() {
        return "PatientHealth{" +
                "heartRate=" + heartRate +
                ", temp=" + temp +
                ", unit='" + unit + '\'' +
                '}';
    }


    public static final class PatientHealthBuilder {
        private int heartRate;
        private Double temp;
        private String unit;

        private PatientHealthBuilder() {
        }

        public static PatientHealthBuilder aPatientHealth() {
            return new PatientHealthBuilder();
        }

        public PatientHealthBuilder withHeartRate(int heartRate) {
            this.heartRate = heartRate;
            return this;
        }

        public PatientHealthBuilder withTemp(Double temp) {
            this.temp = temp;
            return this;
        }

        public PatientHealthBuilder withUnit(String unit) {
            this.unit = unit;
            return this;
        }

        public PatientHealth build() {
            PatientHealth patientHealth = new PatientHealth();
            patientHealth.setHeartRate(heartRate);
            patientHealth.setTemp(temp);
            patientHealth.setUnit(unit);
            return patientHealth;
        }
    }
}
