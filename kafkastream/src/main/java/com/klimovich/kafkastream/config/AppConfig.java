package com.klimovich.kafkastream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "com.klimovich.kafkastream")
public class AppConfig {
    private String applicationId;
    private String bootstrapServers;
    private String heartRateSourceTopic;
    private String temperatureSourceTopic;
    private String patientHealthSinkTopic;
    private int highHeartRateLimit;
    private double highTempRateLimit;

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getHeartRateSourceTopic() {
        return heartRateSourceTopic;
    }

    public void setHeartRateSourceTopic(String heartRateSourceTopic) {
        this.heartRateSourceTopic = heartRateSourceTopic;
    }

    public String getTemperatureSourceTopic() {
        return temperatureSourceTopic;
    }

    public void setTemperatureSourceTopic(String temperatureSourceTopic) {
        this.temperatureSourceTopic = temperatureSourceTopic;
    }

    public String getPatientHealthSinkTopic() {
        return patientHealthSinkTopic;
    }

    public void setPatientHealthSinkTopic(String patientHealthSinkTopic) {
        this.patientHealthSinkTopic = patientHealthSinkTopic;
    }

    public int getHighHeartRateLimit() {
        return highHeartRateLimit;
    }

    public void setHighHeartRateLimit(int highHeartRateLimit) {
        this.highHeartRateLimit = highHeartRateLimit;
    }

    public double getHighTempRateLimit() {
        return highTempRateLimit;
    }

    public void setHighTempRateLimit(double highTempRateLimit) {
        this.highTempRateLimit = highTempRateLimit;
    }
}
