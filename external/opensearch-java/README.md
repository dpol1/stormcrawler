stormcrawler-opensearch-java
===========================

A collection of resources for [OpenSearch](https://opensearch.org/) built on the
[OpenSearch Java Client 3.x](https://opensearch.org/docs/latest/clients/java/) and
Apache HttpClient 5:

* [IndexerBolt](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/bolt/IndexerBolt.java) for indexing documents crawled with StormCrawler
* [Spouts](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/persistence/AggregationSpout.java) and [StatusUpdaterBolt](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/persistence/StatusUpdaterBolt.java) for persisting URL information in recursive crawls
* [MetricsConsumer](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/metrics/MetricsConsumer.java)
* [StatusMetricsBolt](https://github.com/apache/stormcrawler/blob/master/external/opensearch-java/src/main/java/org/apache/stormcrawler/opensearch/metrics/StatusMetricsBolt.java) for sending the breakdown of URLs per status as metrics and display its evolution over time.

This module is functionally equivalent to the legacy `external/opensearch` module
(which is based on the deprecated `RestHighLevelClient` and HttpClient 4), but
uses the typed `OpenSearchClient` and the `ApacheHttpClient5TransportBuilder`
transport. Unlike the legacy client, the Java Client 3.x no longer ships a
sniffer nor a built-in `BulkProcessor`; this module provides an internal
`AsyncBulkProcessor` that preserves the same semantics (size/count/time based
flushing, back-pressure, listener callbacks).

Getting started
---------------------

Add the dependency to your crawler project:

```xml
<dependency>
    <groupId>org.apache.stormcrawler</groupId>
    <artifactId>stormcrawler-opensearch-java</artifactId>
    <version>${stormcrawler.version}</version>
</dependency>
```

You will of course need to have both Storm and OpenSearch installed. For the
latter, see the [OpenSearch documentation](https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/)
for Docker-based setups.

Schemas are automatically created by the bolts on first use; you can override
them by providing your own index definitions before starting the topology.

Configuration and dashboards
---------------------

For a ready-to-use crawler configuration, example Flux topologies, index
initialization scripts and OpenSearch Dashboards exports, refer to the
[`external/opensearch`](../opensearch) module: all of those resources are
compatible with this module and have not been duplicated here.
