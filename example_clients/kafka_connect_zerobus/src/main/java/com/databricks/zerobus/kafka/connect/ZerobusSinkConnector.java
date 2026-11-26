package com.databricks.zerobus.kafka.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/** Kafka Connect sink connector that writes topics to Unity Catalog tables through Zerobus Ingest. */
public class ZerobusSinkConnector extends SinkConnector {

    private Map<String, String> props;

    @Override
    public String version() {
        return "0.2.0-iteration2";
    }

    @Override
    public void start(Map<String, String> props) {
        new ZerobusSinkConfig(props);  // fail fast on invalid configuration
        this.props = Map.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ZerobusSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> out = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> p = new java.util.HashMap<>(props);
            p.put(ZerobusSinkConfig.TASK_INDEX, Integer.toString(i));
            // The control-topic group id is per connector, so two connectors never share a table-service shard.
            p.put(ZerobusSinkConfig.CONNECTOR_NAME, props.getOrDefault("name", "zerobus"));
            out.add(p);
        }
        return out;
    }

    @Override
    public void stop() {
        // Nothing to release: tasks own their clients.
    }

    @Override
    public ConfigDef config() {
        return ZerobusSinkConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config result = super.validate(connectorConfigs);
        try {
            new ZerobusSinkConfig(connectorConfigs);
        } catch (org.apache.kafka.common.config.ConfigException e) {
            for (ConfigValue v : result.configValues()) {
                if (e.getMessage() != null && e.getMessage().contains(v.name())) {
                    v.addErrorMessage(e.getMessage());
                }
            }
        }
        return result;
    }
}
