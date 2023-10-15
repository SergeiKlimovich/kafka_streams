package com.klimovich.kafkastream;

import java.time.Duration;
import java.util.HashMap;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.klimovich.kafkastream.config.AppConfig;
import com.klimovich.kafkastream.joiner.PatientHealthJoiner;
import com.klimovich.kafkastream.model.BodyTemp;
import com.klimovich.kafkastream.model.HeartRate;
import com.klimovich.kafkastream.model.PatientHealth;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
@Configuration
@EnableKafka
@EnableKafkaStreams
public class PatientHealthStream {
    private static final Logger logger = LoggerFactory.getLogger(PatientHealthStream.class);

    private final AppConfig appConfig;

    public PatientHealthStream(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        var props = new HashMap<String, Object>();
        props.put(APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        props.put(BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, true);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, PatientHealth> kStream(StreamsBuilder streamsBuilder,
                                                  Serde<String> stringSerde,
                                                  JsonSerde<HeartRate> heartRateJsonSerde,
                                                  JsonSerde<BodyTemp> bodyTempJsonSerde,
                                                  JsonSerde<PatientHealth> patientVitalsJsonSerde) {

        var heartReteStream = streamsBuilder
                .stream(appConfig.getHeartRateSourceTopic(), Consumed.with(stringSerde, heartRateJsonSerde))
                .peek((k, v) -> logger.info("Received new heart rate for patient: {} : {}", k, v));

        var bodyTempStream = streamsBuilder
                .stream(appConfig.getTemperatureSourceTopic(), Consumed.with(stringSerde, bodyTempJsonSerde))
                .peek((k, v) -> logger.info("Received new heart rate for patient: {} : {}", k, v));

        var highPulseStream = heartReteStream
                .filter((key, value) -> value != null && value.getPulsePerMinute() != null && value.getPulsePerMinute() >= appConfig.getHighHeartRateLimit());

        var highBodyTempStream = bodyTempStream
                .filter((key, value) -> value != null && value.getTemp() != null && value.getTemp() >= appConfig.getHighTempRateLimit());

        var patientVitalsJoiner = new PatientHealthJoiner();


        var joinWindow = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(10));

        var patientVitalsJoinedStream = highPulseStream
                .join(highBodyTempStream,
                        patientVitalsJoiner,
                        joinWindow,
                        StreamJoined.with(stringSerde, heartRateJsonSerde, bodyTempJsonSerde)
                                .withName("patient-vitals-join").withStoreName("patient-vitals-join-store")
                );

        patientVitalsJoinedStream.peek((key, value) -> logger.info("key[{}] value[{}]", key, value))
                .to(appConfig.getPatientHealthSinkTopic(), Produced.with(stringSerde, patientVitalsJsonSerde));

        logger.info("topology: {}", streamsBuilder.build().describe());

        return patientVitalsJoinedStream;
    }

    @Bean
    public Serde<String> stringSerde() {
        return Serdes.String();
    }

    @Bean
    public JsonSerde<HeartRate> heartRateJsonSerde() {
        return new JsonSerde<>(HeartRate.class);
    }

    @Bean
    public JsonSerde<BodyTemp> bodyTempJsonSerde() {
        return new JsonSerde<>(BodyTemp.class);
    }

    @Bean
    public JsonSerde<PatientHealth> patientVitalsJsonSerde() {
        return new JsonSerde<>(PatientHealth.class);
    }
}
