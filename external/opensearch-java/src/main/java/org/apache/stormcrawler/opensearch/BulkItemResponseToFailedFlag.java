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

import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

public final class BulkItemResponseToFailedFlag {
    @NotNull public final BulkResponseItem response;
    public final boolean failed;
    @NotNull public final String id;

    public BulkItemResponseToFailedFlag(@NotNull BulkResponseItem response, boolean failed) {
        this.response = response;
        this.failed = failed;
        this.id = Objects.requireNonNull(response.id(), "BulkResponseItem id must not be null");
    }

    /** Returns the error cause, or {@code null} if the item did not fail. */
    @Nullable
    public ErrorCause getFailedCause() {
        return response.error();
    }

    /** Returns a human-readable failure description, or {@code null} if the item did not fail. */
    @Nullable
    public String getFailure() {
        ErrorCause error = response.error();
        if (error == null) {
            return null;
        }
        return error.reason() != null ? error.reason() : error.type();
    }

    public Integer getStatus() {
        return response.status();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BulkItemResponseToFailedFlag)) {
            return false;
        }

        BulkItemResponseToFailedFlag that = (BulkItemResponseToFailedFlag) o;

        if (failed != that.failed) {
            return false;
        }
        if (!response.equals(that.response)) {
            return false;
        }
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = response.hashCode();
        result = 31 * result + (failed ? 1 : 0);
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BulkItemResponseToFailedFlag{"
                + "response="
                + response
                + ", failed="
                + failed
                + ", id='"
                + id
                + '\''
                + '}';
    }
}
