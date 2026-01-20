CREATE TABLE IF NOT EXISTS bronze.security_events_kafka (
  raw String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = '10.110.12.20:9092',
  kafka_topic_list = 'malcolm-logs',
  kafka_group_name = 'security_events_ch',
  kafka_format = 'JSONAsString',
  kafka_num_consumers = 1,
  kafka_skip_broken_messages = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.suricata_events_mv
TO bronze.suricata_events_raw
AS
SELECT
  JSONExtractString(raw, 'event.hash') AS event_id,
  coalesce(
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, '@timestamp'), '')),
    parseDateTime64BestEffortOrNull(JSONExtractString(raw, 'suricata.timestamp'))
  ) AS event_ts,
  coalesce(
    JSONExtractString(raw, 'event.provider'),
    JSONExtractString(raw, 'event.module')
  ) AS sensor_type,
  coalesce(
    JSONExtractString(raw, 'agent.name'),
    JSONExtractString(raw, 'host.name'),
    JSONExtractString(raw, 'node')
  ) AS sensor_name,
  coalesce(
    JSONExtractString(raw, 'event.dataset'),
    JSONExtractString(raw, 'event.kind')
  ) AS event_type,
  coalesce(
    JSONExtractString(raw, 'suricata.alert.severity'),
    JSONExtractString(raw, 'event.severity')
  ) AS severity,
  toIPv6OrNull(JSONExtractString(raw, 'source.ip')) AS src_ip,
  toIPv6OrNull(JSONExtractString(raw, 'destination.ip')) AS dest_ip,
  toInt32OrNull(JSONExtractString(raw, 'source.port')) AS src_port,
  toInt32OrNull(JSONExtractString(raw, 'destination.port')) AS dest_port,
  coalesce(
    JSONExtractString(raw, 'network.application'),
    JSONExtractString(raw, 'network.transport.0'),
    JSONExtractString(raw, 'network.protocol.0'),
    JSONExtractString(raw, 'protocol.0')
  ) AS protocol,
  coalesce(
    toInt64OrNull(JSONExtractString(raw, 'totDataBytes')),
    toInt64OrNull(JSONExtractString(raw, 'network.bytes')),
    toInt64OrNull(JSONExtractString(raw, 'client.bytes')),
    toInt64OrNull(JSONExtractString(raw, 'server.bytes'))
  ) AS bytes,
  coalesce(
    toInt64OrNull(JSONExtractString(raw, 'network.packets')),
    toInt64OrNull(JSONExtractString(raw, 'client.packets')),
    toInt64OrNull(JSONExtractString(raw, 'server.packets'))
  ) AS packets,
  JSONExtractString(raw, 'suricata.flow_id') AS flow_id,
  coalesce(
    JSONExtractString(raw, 'rule.name'),
    JSONExtractString(raw, 'suricata.alert.signature')
  ) AS signature,
  toInt32OrNull(JSONExtractString(raw, 'rule.id')) AS signature_id,
  JSONExtractString(raw, 'rule.category.0') AS category,
  JSONExtractString(raw, 'suricata.alert.action') AS alert_action,
  JSONExtractString(raw, 'suricata.http.url') AS http_url,
  ifNull(
    JSONExtract(raw, 'tags', 'Array(String)'),
    ifNull(JSONExtract(raw, 'event.severity_tags', 'Array(String)'), [])
  ) AS tags,
  coalesce(
    JSONExtractString(raw, 'message'),
    JSONExtractString(raw, 'event.original'),
    JSONExtractString(raw, 'rule.name')
  ) AS message,
  raw AS raw_data
FROM bronze.security_events_kafka
WHERE JSONHas(raw, 'suricata')
  AND JSONExtractString(raw, 'event.hash') != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.wazuh_events_mv
TO bronze.wazuh_events_raw
AS
SELECT
  JSONExtractString(raw, 'event.hash') AS event_id,
  coalesce(
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, '@timestamp'), '')),
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, 'event.ingested'), '')),
    fromUnixTimestamp64Milli(toInt64OrNull(JSONExtractString(raw, 'event.start'))),
    fromUnixTimestamp64Milli(toInt64OrNull(JSONExtractString(raw, 'event.end')))
  ) AS event_ts,
  parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, 'event.ingested'), '')) AS event_ingested_ts,
  fromUnixTimestamp64Milli(toInt64OrNull(JSONExtractString(raw, 'event.start'))) AS event_start_ts,
  fromUnixTimestamp64Milli(toInt64OrNull(JSONExtractString(raw, 'event.end'))) AS event_end_ts,
  JSONExtractString(raw, 'event.dataset') AS event_dataset,
  JSONExtractString(raw, 'event.kind') AS event_kind,
  JSONExtractString(raw, 'event.module') AS event_module,
  JSONExtractString(raw, 'event.provider') AS event_provider,
  JSONExtractString(raw, 'agent.name') AS agent_name,
  toIPv6OrNull(JSONExtractString(raw, 'agent.ip')) AS agent_ip,
  JSONExtractString(raw, 'host.name') AS host_name,
  toIPv6OrNull(JSONExtractString(raw, 'host.ip')) AS host_ip,
  JSONExtractString(raw, 'rule.id') AS rule_id,
  toInt32OrNull(JSONExtractString(raw, 'rule.level')) AS rule_level,
  JSONExtractString(raw, 'rule.name') AS rule_name,
  JSONExtractRaw(raw, 'rule.ruleset') AS rule_ruleset,
  ifNull(JSONExtract(raw, 'tags', 'Array(String)'), []) AS tags,
  coalesce(JSONExtractString(raw, 'message'), JSONExtractString(raw, 'rule.name')) AS message,
  raw AS raw_data
FROM bronze.security_events_kafka
WHERE JSONExtractString(raw, 'event.provider') = 'wazuh'
  AND JSONExtractString(raw, 'event.hash') != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.zeek_events_mv
TO bronze.zeek_events_raw
AS
SELECT
  JSONExtractString(raw, 'event.hash') AS event_id,
  coalesce(
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, '@timestamp'), '')),
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, 'zeek.ts'), '')),
    parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, 'event.ingested'), '')),
    fromUnixTimestamp64Milli(toInt64OrNull(JSONExtractString(raw, 'event.start')))
  ) AS event_ts,
  parseDateTime64BestEffortOrNull(nullIf(JSONExtractString(raw, 'event.ingested'), '')) AS event_ingested_ts,
  fromUnixTimestamp64Milli(toInt64OrNull(JSONExtractString(raw, 'event.start'))) AS event_start_ts,
  fromUnixTimestamp64Milli(toInt64OrNull(JSONExtractString(raw, 'event.end'))) AS event_end_ts,
  JSONExtractString(raw, 'event.dataset') AS event_dataset,
  JSONExtractString(raw, 'event.kind') AS event_kind,
  JSONExtractString(raw, 'event.module') AS event_module,
  JSONExtractString(raw, 'event.provider') AS event_provider,
  coalesce(
    JSONExtractString(raw, 'zeek.uid'),
    JSONExtractString(raw, 'event.id.0')
  ) AS zeek_uid,
  coalesce(
    JSONExtractString(raw, 'agent.name'),
    JSONExtractString(raw, 'host.name'),
    JSONExtractString(raw, 'node')
  ) AS sensor_name,
  toIPv6OrNull(JSONExtractString(raw, 'source.ip')) AS src_ip,
  toIPv6OrNull(JSONExtractString(raw, 'destination.ip')) AS dest_ip,
  toInt32OrNull(JSONExtractString(raw, 'source.port')) AS src_port,
  toInt32OrNull(JSONExtractString(raw, 'destination.port')) AS dest_port,
  coalesce(
    JSONExtractString(raw, 'network.application'),
    JSONExtractString(raw, 'network.transport.0'),
    JSONExtractString(raw, 'network.protocol.0'),
    JSONExtractString(raw, 'protocol.0')
  ) AS protocol,
  JSONExtractString(raw, 'network.application') AS application,
  JSONExtractString(raw, 'network.type') AS network_type,
  JSONExtractString(raw, 'network.direction') AS direction,
  JSONExtractString(raw, 'network.community_id') AS community_id,
  coalesce(
    toInt64OrNull(JSONExtractString(raw, 'totDataBytes')),
    toInt64OrNull(JSONExtractString(raw, 'network.bytes')),
    toInt64OrNull(JSONExtractString(raw, 'source.bytes')),
    toInt64OrNull(JSONExtractString(raw, 'destination.bytes'))
  ) AS bytes,
  coalesce(
    toInt64OrNull(JSONExtractString(raw, 'network.packets')),
    toInt64OrNull(JSONExtractString(raw, 'source.packets')),
    toInt64OrNull(JSONExtractString(raw, 'destination.packets'))
  ) AS packets,
  coalesce(
    toInt64OrNull(JSONExtractString(raw, 'zeek.conn.orig_bytes')),
    toInt64OrNull(JSONExtractString(raw, 'zeek.conn.orig_ip_bytes'))
  ) AS orig_bytes,
  coalesce(
    toInt64OrNull(JSONExtractString(raw, 'zeek.conn.resp_bytes')),
    toInt64OrNull(JSONExtractString(raw, 'zeek.conn.resp_ip_bytes'))
  ) AS resp_bytes,
  toInt64OrNull(JSONExtractString(raw, 'zeek.conn.orig_pkts')) AS orig_pkts,
  toInt64OrNull(JSONExtractString(raw, 'zeek.conn.resp_pkts')) AS resp_pkts,
  JSONExtractString(raw, 'zeek.conn.conn_state') AS conn_state,
  JSONExtractString(raw, 'zeek.conn.conn_state_description') AS conn_state_description,
  toFloat64OrNull(JSONExtractString(raw, 'zeek.conn.duration')) AS duration,
  JSONExtractString(raw, 'zeek.conn.history') AS history,
  coalesce(
    JSONExtractString(raw, 'zeek.conn.vlan'),
    JSONExtractString(raw, 'network.vlan.id.0')
  ) AS vlan_id,
  ifNull(
    JSONExtract(raw, 'tags', 'Array(String)'),
    ifNull(
      JSONExtract(raw, 'event.category', 'Array(String)'),
      ifNull(JSONExtract(raw, 'event.severity_tags', 'Array(String)'), [])
    )
  ) AS tags,
  coalesce(
    JSONExtractString(raw, 'message'),
    JSONExtractString(raw, 'event.original'),
    JSONExtractString(raw, 'zeek.conn.conn_state_description')
  ) AS message,
  raw AS raw_data
FROM bronze.security_events_kafka
WHERE JSONHas(raw, 'zeek')
  AND JSONExtractString(raw, 'event.hash') != '';
