package io.confluent.developer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.KeyValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.developer.avro.Rating;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static java.util.Arrays.asList;


public class RekeyValueTest {

private final static String TEST_CONFIG_FILE = "configuration/test.properties";

  private TopologyTestDriver testDriver;

  private SpecificAvroSerde<Rating> makeSerializer(Properties allProps) {
    SpecificAvroSerde<Rating> serde = new SpecificAvroSerde<>();
    Map<String, String> config = new HashMap<>();
    config.put("schema.registry.url", allProps.getProperty("schema.registry.url"));
    serde.configure(config, false);
    return serde;
  }

  @Test
  public void shouldRekey() throws IOException {
    RekeyValue rk = new RekeyValue();
    Properties allProps = rk.loadEnvProperties(TEST_CONFIG_FILE);

    String inputTopic = allProps.getProperty("input.topic.name");
    String outputTopic = allProps.getProperty("output.topic.name");

    final SpecificAvroSerde<Rating> RatingpecificAvroSerde = makeSerializer(allProps);

    Topology topology = rk.buildTopology(allProps, RatingpecificAvroSerde);
    testDriver = new TopologyTestDriver(topology, allProps);

    Serializer<Integer> keySerializer = Serdes.Integer().serializer();
    Deserializer<Integer> keyDeserializer = Serdes.Integer().deserializer();

    // Fixture
    Rating old1 = new Rating(1,294, 8.2);
    Rating old2 = new Rating(2,294, 8.5);
    Rating old3 = new Rating(3,354, 9.9);
    Rating old4 = new Rating(4,354, 9.7);
    Rating old5 = new Rating(5,782, 7.8);
    Rating old6 = new Rating(6,782, 7.7);
    Rating old7 = new Rating(7,128, 8.7);
    Rating old8 = new Rating(8,128, 8.4);
    Rating old9 = new Rating(9,780, 2.1);

    KeyValue<Integer, Rating> new1 = new KeyValue(294, old1);
    KeyValue<Integer, Rating> new2 = new KeyValue(294, old2);
    KeyValue<Integer, Rating> new3 = new KeyValue(354, old3);
    KeyValue<Integer, Rating> new4 = new KeyValue(354, old4);
    KeyValue<Integer, Rating> new5 = new KeyValue(782, old5);
    KeyValue<Integer, Rating> new6 = new KeyValue(782, old6);
    KeyValue<Integer, Rating> new7 = new KeyValue(128, old7);
    KeyValue<Integer, Rating> new8 = new KeyValue(128, old8);
    KeyValue<Integer, Rating> new9 = new KeyValue(780, old9);
    // end Fixture

    final List<Rating> input = asList(old1, old2, old3, old4, old5, old6, old7, old8, old9);
    final List<KeyValue<Integer,Rating>> expectedOutput = asList(new1, new2, new3, new4, new5, new6, new7, new8, new9);

    testDriver.createInputTopic(inputTopic, keySerializer, RatingpecificAvroSerde.serializer())
        .pipeValueList(input);
    
    List<KeyValue<Integer, Rating>> actualOutput = testDriver.createOutputTopic(outputTopic, keyDeserializer, RatingpecificAvroSerde.deserializer()).readKeyValuesToList();

    Assert.assertTrue(actualOutput.equals(expectedOutput));
  }

  @After
  public void cleanup() {
    testDriver.close();
  }
}
