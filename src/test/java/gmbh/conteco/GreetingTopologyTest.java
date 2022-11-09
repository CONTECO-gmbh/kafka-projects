package gmbh.conteco;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

// Unit Testing mit JUnit
public class GreetingTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<Void, String> inputTopic;
    private TestOutputTopic<Void, String> outputTopic;

    @BeforeEach
    void setup() {
        Topology topology = GreetingTopology.build();

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, GreetingTopologyTest.class.getName());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(topology, properties);

        inputTopic = testDriver.createInputTopic("persons", Serdes.Void().serializer(),
                Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("greetings", Serdes.Void().deserializer(),
                Serdes.String().deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testGreeting_isNotEmpty() {
        inputTopic.pipeInput("Izzy");
        assertThat(outputTopic.isEmpty()).isFalse();
    }

    @Test
    void testGreeting_ignoreJim() {
        inputTopic.pipeInput("Izzy");
        inputTopic.pipeInput("Jim");

        var records = outputTopic.readRecordsToList();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).getValue()).isEqualTo("Hallo Izzy");
    }
}