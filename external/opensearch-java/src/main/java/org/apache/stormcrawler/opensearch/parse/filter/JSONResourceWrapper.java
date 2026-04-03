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

package org.apache.stormcrawler.opensearch.parse.filter;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Wraps a ParseFilter whose resources are in a JSON file that can be stored in OpenSearch. The
 * benefit of doing this is that the resources can be refreshed automatically and modified without
 * having to recompile the jar and restart the topology. The connection to OpenSearch is done via
 * the config and uses a new bolt type 'config'.
 *
 * <p>The configuration of the delegate is done in the parsefilters.json as usual.
 *
 * <pre>
 *  {
 *     "class": "org.apache.stormcrawler.opensearch.parse.filter.JSONResourceWrapper",
 *     "name": "OpenSearchCollectionTagger",
 *     "params": {
 *         "refresh": "60",
 *         "delegate": {
 *             "class": "org.apache.stormcrawler.parse.filter.CollectionTagger",
 *             "params": {
 *                 "file": "collections.json"
 *             }
 *         }
 *     }
 *  }
 * </pre>
 *
 * The resource file can be pushed to OpenSearch with
 *
 * <pre>
 *  curl -XPUT "$OSHOST/config/_create/collections.json" -H 'Content-Type: application/json' -d @src/main/resources/collections.json
 * </pre>
 */
public class JSONResourceWrapper extends ParseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(JSONResourceWrapper.class);

    private ParseFilter delegatedParseFilter;
    private Timer refreshTimer;
    private OpenSearchClient osClient;

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {

        String parsefilterclass = null;

        JsonNode delegateNode = filterParams.get("delegate");
        if (delegateNode == null) {
            throw new RuntimeException("delegateNode undefined!");
        }

        JsonNode node = delegateNode.get("class");
        if (node != null && node.isTextual()) {
            parsefilterclass = node.asText();
        }

        if (parsefilterclass == null) {
            throw new RuntimeException("parsefilter.class undefined!");
        }

        // load an instance of the delegated parsefilter
        try {
            Class<?> filterClass = Class.forName(parsefilterclass);

            boolean subClassOK = ParseFilter.class.isAssignableFrom(filterClass);
            if (!subClassOK) {
                throw new RuntimeException(
                        "Filter " + parsefilterclass + " does not extend ParseFilter");
            }

            delegatedParseFilter = (ParseFilter) filterClass.getDeclaredConstructor().newInstance();

            // check that it implements JSONResource
            if (!JSONResource.class.isInstance(delegatedParseFilter)) {
                throw new RuntimeException(
                        "Filter " + parsefilterclass + " does not implement JSONResource");
            }

        } catch (Exception e) {
            LOG.error("Can't setup {}: {}", parsefilterclass, e);
            throw new RuntimeException("Can't setup " + parsefilterclass, e);
        }

        // configure it
        node = delegateNode.get("params");

        delegatedParseFilter.configure(stormConf, node);

        int refreshRate = 600;

        node = filterParams.get("refresh");
        if (node != null && node.isInt()) {
            refreshRate = node.asInt(refreshRate);
        }

        final JSONResource resource = (JSONResource) delegatedParseFilter;

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
    public void filter(String URL, byte[] content, DocumentFragment doc, ParseResult parse) {
        delegatedParseFilter.filter(URL, content, doc, parse);
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
