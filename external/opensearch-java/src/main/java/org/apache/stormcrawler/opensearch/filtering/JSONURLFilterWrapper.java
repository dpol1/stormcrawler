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

package org.apache.stormcrawler.opensearch.filtering;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a URLFilter whose resources are in a JSON file that can be stored in OpenSearch. The
 * benefit of doing this is that the resources can be refreshed automatically and modified without
 * having to recompile the jar and restart the topology. The connection to OpenSearch is done via
 * the config and uses a new bolt type 'config'.
 *
 * <p>The configuration of the delegate is done in the urlfilters.json as usual.
 *
 * <pre>
 *  {
 *     "class": "org.apache.stormcrawler.opensearch.filtering.JSONURLFilterWrapper",
 *     "name": "OSFastURLFilter",
 *     "params": {
 *         "refresh": "60",
 *         "delegate": {
 *             "class": "org.apache.stormcrawler.filtering.regex.FastURLFilter",
 *             "params": {
 *                 "file": "fast.urlfilter.json"
 *             }
 *         }
 *     }
 *  }
 * </pre>
 *
 * The resource file can be pushed to OpenSearch with
 *
 * <pre>
 *  curl -XPUT 'localhost:9200/config/config/fast.urlfilter.json?pretty' -H 'Content-Type: application/json' -d @fast.urlfilter.json
 * </pre>
 */
public class JSONURLFilterWrapper extends URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(JSONURLFilterWrapper.class);

    private URLFilter delegatedURLFilter;
    private Timer refreshTimer;
    private OpenSearchClient osClient;

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {

        String urlfilterclass = null;

        JsonNode delegateNode = filterParams.get("delegate");
        if (delegateNode == null) {
            throw new RuntimeException("delegateNode undefined!");
        }

        JsonNode node = delegateNode.get("class");
        if (node != null && node.isTextual()) {
            urlfilterclass = node.asText();
        }

        if (urlfilterclass == null) {
            throw new RuntimeException("urlfilter.class undefined!");
        }

        // load an instance of the delegated parsefilter
        try {
            Class<?> filterClass = Class.forName(urlfilterclass);

            boolean subClassOK = URLFilter.class.isAssignableFrom(filterClass);
            if (!subClassOK) {
                throw new RuntimeException(
                        "Filter " + urlfilterclass + " does not extend URLFilter");
            }

            delegatedURLFilter = (URLFilter) filterClass.getDeclaredConstructor().newInstance();

            // check that it implements JSONResource
            if (!JSONResource.class.isInstance(delegatedURLFilter)) {
                throw new RuntimeException(
                        "Filter " + urlfilterclass + " does not implement JSONResource");
            }

        } catch (Exception e) {
            LOG.error("Can't setup {}: {}", urlfilterclass, e);
            throw new RuntimeException("Can't setup " + urlfilterclass, e);
        }

        // configure it
        node = delegateNode.get("params");

        delegatedURLFilter.configure(stormConf, node);

        int refreshRate = 600;

        node = filterParams.get("refresh");
        if (node != null && node.isInt()) {
            refreshRate = node.asInt(refreshRate);
        }

        final JSONResource resource = (JSONResource) delegatedURLFilter;

        refreshTimer = new Timer();
        refreshTimer.schedule(
                new TimerTask() {
                    public void run() {
                        if (osClient == null) {
                            try {
                                osClient = OpenSearchConnection.getClient(stormConf, "config");
                            } catch (Exception e) {
                                LOG.error("Exception while creating OpenSearch connection", e);
                            }
                        }
                        if (osClient != null) {
                            LOG.info("Reloading json resources from OpenSearch");
                            try {
                                GetResponse<JsonData> response =
                                        osClient.get(
                                                g ->
                                                        g.index("config")
                                                                .id(resource.getResourceFile()),
                                                JsonData.class);
                                if (response.found() && response.source() != null) {
                                    String json = response.source().toJson().toString();
                                    resource.loadJSONResources(
                                            new ByteArrayInputStream(
                                                    json.getBytes(StandardCharsets.UTF_8)));
                                }
                            } catch (Exception e) {
                                LOG.error("Can't load config from OpenSearch", e);
                            }
                        }
                    }
                },
                0,
                refreshRate * 1000);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        return delegatedURLFilter.filter(sourceUrl, sourceMetadata, urlToFilter);
    }

    @Override
    public void cleanup() {
        if (refreshTimer != null) {
            refreshTimer.cancel();
        }
        if (osClient != null) {
            try {
                osClient._transport().close();
            } catch (IOException e) {
                LOG.error("Exception when closing OpenSearch client", e);
            }
        }
    }
}
