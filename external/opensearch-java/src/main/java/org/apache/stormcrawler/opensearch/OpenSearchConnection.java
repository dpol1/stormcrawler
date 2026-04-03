/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.opensearch;

import static org.opensearch.client.RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static org.opensearch.client.RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.client.HttpAsyncResponseConsumerFactory;
import org.opensearch.client.Node;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.sniff.Sniffer;
import org.opensearch.client.transport.rest_client.RestClientOptions;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to instantiate an OpenSearch client and bulkprocessor based on the configuration.
 */
public final class OpenSearchConnection {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchConnection.class);

    @NotNull private final OpenSearchClient client;

    @NotNull private final AsyncBulkProcessor processor;

    @Nullable private final Sniffer sniffer;

    @NotNull private final RestClient restClient;

    private OpenSearchConnection(
            @NotNull OpenSearchClient c,
            @NotNull AsyncBulkProcessor p,
            @Nullable Sniffer s,
            @NotNull RestClient rc) {
        client = c;
        processor = p;
        sniffer = s;
        restClient = rc;
    }

    public OpenSearchClient getClient() {
        return client;
    }

    /**
     * Creates a standalone {@link OpenSearchClient}. Used by classes that need a client without a
     * bulk processor (e.g. spouts, filters). Callers are responsible for closing the returned
     * client's transport via {@code client._transport().close()}.
     */
    public static OpenSearchClient getClient(Map<String, Object> stormConf, String boltType) {
        return buildClientResources(stormConf, boltType, 100).client();
    }

    /** Adds a single bulk operation to the internal processor. */
    public void addToProcessor(final BulkOperation operation) {
        processor.add(operation);
    }

    /**
     * Creates a connection with a default (no-op) listener. The values for bolt type are
     * [indexer,status,metrics].
     */
    public static OpenSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType) {
        AsyncBulkProcessor.Listener listener =
                new AsyncBulkProcessor.Listener() {
                    @Override
                    public void afterBulk(
                            long arg0,
                            org.opensearch.client.opensearch.core.BulkRequest arg1,
                            org.opensearch.client.opensearch.core.BulkResponse arg2) {}

                    @Override
                    public void afterBulk(
                            long arg0,
                            org.opensearch.client.opensearch.core.BulkRequest arg1,
                            Throwable arg2) {}

                    @Override
                    public void beforeBulk(
                            long arg0, org.opensearch.client.opensearch.core.BulkRequest arg1) {}
                };
        return getConnection(stormConf, boltType, listener);
    }

    public static OpenSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType, AsyncBulkProcessor.Listener listener) {

        final String dottedType = boltType + ".";

        final int bufferSize =
                ConfUtils.getInt(
                        stormConf, Constants.PARAMPREFIX, dottedType, "responseBufferSize", 100);

        ClientResources cr = buildClientResources(stormConf, boltType, bufferSize);

        final String flushIntervalString =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX, dottedType, "flushInterval", "5s");

        final long flushIntervalMillis = parseTimeValueToMillis(flushIntervalString, 5000);

        final int bulkActions =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX, dottedType, "bulkActions", 50);

        final int concurrentRequests =
                ConfUtils.getInt(
                        stormConf, Constants.PARAMPREFIX, dottedType, "concurrentRequests", 1);

        AsyncBulkProcessor bulkProcessor = null;
        Sniffer sniffer = null;
        try {
            bulkProcessor =
                    new AsyncBulkProcessor.Builder(cr.client(), listener)
                            .setBulkActions(bulkActions)
                            .setFlushIntervalMillis(flushIntervalMillis)
                            .setConcurrentRequests(concurrentRequests)
                            .build();

            boolean sniff =
                    ConfUtils.getBoolean(
                            stormConf, Constants.PARAMPREFIX, dottedType, "sniff", true);
            if (sniff) {
                sniffer = Sniffer.builder(cr.restClient()).build();
            }

            return new OpenSearchConnection(cr.client(), bulkProcessor, sniffer, cr.restClient());
        } catch (Exception e) {
            if (bulkProcessor != null) {
                try {
                    bulkProcessor.close();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            try {
                cr.restClient().close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public void close() {

        if (!isClosed.compareAndSet(false, true)) {
            LOG.warn("Tried to close an already closed connection!");
            return;
        }

        LOG.debug("Start closing the OpenSearch connection");

        // First, close the BulkProcessor ensuring pending actions are flushed
        try {
            boolean success = processor.awaitClose(60, TimeUnit.SECONDS);
            if (!success) {
                throw new RuntimeException(
                        "Failed to flush pending actions when closing BulkProcessor");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (sniffer != null) {
            sniffer.close();
        }

        // Now close the REST client (also closes the transport)
        try {
            restClient.close();
        } catch (IOException e) {
            LOG.trace("Client threw IO exception.");
        }
    }

    /**
     * Extracts the document ID from a {@link BulkOperation} regardless of its type (index, create,
     * delete, update).
     */
    public static String getBulkOperationId(BulkOperation op) {
        if (op.isIndex()) {
            return op.index().id();
        }
        if (op.isCreate()) {
            return op.create().id();
        }
        if (op.isDelete()) {
            return op.delete().id();
        }
        if (op.isUpdate()) {
            return op.update().id();
        }
        return null;
    }

    // internal helpers
    private record ClientResources(OpenSearchClient client, RestClient restClient) {}

    private static ClientResources buildClientResources(
            Map<String, Object> stormConf, String boltType, int responseBufferSizeMB) {

        final String dottedType = boltType + ".";

        final List<HttpHost> hosts = new ArrayList<>();

        final List<String> confighosts =
                ConfUtils.loadListFromConf(
                        Constants.PARAMPREFIX, dottedType, "addresses", stormConf);

        // find ; separated values and tokenise as multiple addresses
        // e.g. opensearch1:9200; opensearch2:9200
        if (confighosts.size() == 1) {
            String input = confighosts.get(0);
            confighosts.clear();
            confighosts.addAll(Arrays.asList(input.split(" *; *")));
        }

        for (String host : confighosts) {
            // no port specified? use default one
            int port = 9200;
            String scheme = "http";
            // no scheme specified? use http
            if (!host.startsWith(scheme)) {
                host = "http://" + host;
            }
            URI uri = URI.create(host);
            if (uri.getHost() == null) {
                throw new RuntimeException("host undefined " + host);
            }
            if (uri.getPort() != -1) {
                port = uri.getPort();
            }
            if (uri.getScheme() != null) {
                scheme = uri.getScheme();
            }
            hosts.add(new HttpHost(uri.getHost(), port, scheme));
        }

        final RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));

        // authentication via user / password
        final String user =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX, dottedType, "user");
        final String password =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX, dottedType, "password");

        final String proxyhost =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX, dottedType, "proxy.host");

        final int proxyport =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX, dottedType, "proxy.port", -1);

        final String proxyscheme =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX, dottedType, "proxy.scheme", "http");

        final boolean disableTlsValidation =
                ConfUtils.getBoolean(
                        stormConf, Constants.PARAMPREFIX, "", "disable.tls.validation", false);

        final boolean needsUser = StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password);
        final boolean needsProxy = StringUtils.isNotBlank(proxyhost) && proxyport != -1;

        if (needsUser || needsProxy || disableTlsValidation) {
            builder.setHttpClientConfigCallback(
                    httpClientBuilder -> {
                        if (needsUser) {
                            final CredentialsProvider credentialsProvider =
                                    new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(
                                    AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                        if (needsProxy) {
                            httpClientBuilder.setProxy(
                                    new HttpHost(proxyhost, proxyport, proxyscheme));
                        }

                        if (disableTlsValidation) {
                            try {
                                final SSLContextBuilder sslContext = new SSLContextBuilder();
                                sslContext.loadTrustMaterial(null, new TrustAllStrategy());
                                httpClientBuilder.setSSLContext(sslContext.build());
                                httpClientBuilder.setSSLHostnameVerifier(
                                        NoopHostnameVerifier.INSTANCE);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to disable TLS validation", e);
                            }
                        }
                        return httpClientBuilder;
                    });
        }

        final int connectTimeout =
                ConfUtils.getInt(
                        stormConf,
                        Constants.PARAMPREFIX,
                        dottedType,
                        "connect.timeout",
                        DEFAULT_CONNECT_TIMEOUT_MILLIS);
        final int socketTimeout =
                ConfUtils.getInt(
                        stormConf,
                        Constants.PARAMPREFIX,
                        dottedType,
                        "socket.timeout",
                        DEFAULT_SOCKET_TIMEOUT_MILLIS);
        // timeout until connection is established
        builder.setRequestConfigCallback(
                requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(connectTimeout)
                                // Timeout when waiting for data
                                .setSocketTimeout(socketTimeout));

        // TODO check if this has gone somewhere else
        // int maxRetryTimeout = ConfUtils.getInt(stormConf, Constants.PARAMPREFIX +
        // boltType +
        // ".max.retry.timeout",
        // DEFAULT_MAX_RETRY_TIMEOUT_MILLIS);
        // builder.setMaxRetryTimeoutMillis(maxRetryTimeout);

        // TODO configure headers etc...
        // Map<String, String> configSettings = (Map) stormConf
        // .get(Constants.PARAMPREFIX + boltType + ".settings");
        // if (configSettings != null) {
        // configSettings.forEach((k, v) -> settings.put(k, v));
        // }

        // use node selector only to log nodes listed in the config
        // and/or discovered through sniffing
        builder.setNodeSelector(
                nodes -> {
                    for (Node node : nodes) {
                        LOG.debug(
                                "Connected to OpenSearch node {} [{}] for {}",
                                node.getName(),
                                node.getHost(),
                                boltType);
                    }
                });

        final boolean compression =
                ConfUtils.getBoolean(
                        stormConf, Constants.PARAMPREFIX, dottedType, "compression", false);

        builder.setCompressionEnabled(compression);

        final RestClient restClient = builder.build();

        // --- Response buffer size configuration ---
        // The default HeapBufferedResponseConsumerFactory in the low-level REST client has
        // a hardcoded limit of 100 MB. Large MSearch or aggregation responses can exceed
        // this, causing ContentTooLongException.
        //
        // This fix works because we use RestClientTransport, which passes RequestOptions
        // (including HttpAsyncResponseConsumerFactory) directly to the low-level RestClient.
        //
        // NOTE: if StormCrawler ever switches to ApacheHttpClient5Transport, this approach
        // will silently stop working. In that case, use:
        //   ApacheHttpClient5Options.DEFAULT.toBuilder()
        //       .setHttpAsyncResponseConsumerFactory(factory).build()
        // See: https://github.com/opensearch-project/opensearch-java/issues/1370
        final int DEFAULT_RESPONSE_BUFFER_SIZE_MB = 100;
        final int effectiveBufferSizeMB;
        if (responseBufferSizeMB <= 0) {
            LOG.warn(
                    "Invalid responseBufferSize {}MB for {}, falling back to default {}MB",
                    responseBufferSizeMB,
                    boltType,
                    DEFAULT_RESPONSE_BUFFER_SIZE_MB);
            effectiveBufferSizeMB = DEFAULT_RESPONSE_BUFFER_SIZE_MB;
        } else {
            effectiveBufferSizeMB = responseBufferSizeMB;
        }
        LOG.info("OpenSearch response buffer size for {}: {}MB", boltType, effectiveBufferSizeMB);

        final RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(
                        effectiveBufferSizeMB * 1024 * 1024));
        final RestClientOptions transportOptions = new RestClientOptions(optionsBuilder.build());

        final RestClientTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper(), transportOptions);
        final OpenSearchClient openSearchClient = new OpenSearchClient(transport);

        return new ClientResources(openSearchClient, restClient);
    }

    /**
     * Parses a time value string (e.g. "5s", "500ms", "1m") into milliseconds.
     *
     * @param value the string to parse
     * @param defaultMillis the default if parsing fails
     * @return milliseconds
     */
    static long parseTimeValueToMillis(String value, long defaultMillis) {
        if (value == null || value.isBlank()) {
            return defaultMillis;
        }
        value = value.trim();
        try {
            if (value.endsWith("ms")) {
                return Long.parseLong(value.substring(0, value.length() - 2));
            } else if (value.endsWith("s")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1000;
            } else if (value.endsWith("m")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 60000;
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse time value '{}', using default {}ms", value, defaultMillis);
            return defaultMillis;
        }
    }
}
