/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal.rowtracking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.metadatadomain.JsonMetadataDomain;
import java.util.Optional;

/** Represents the metadata domain for row tracking. */
public class RowTrackingMetadataDomain extends JsonMetadataDomain {

  /**
   * Creates an instance of {@link RowTrackingMetadataDomain} from a JSON configuration string.
   *
   * @param json the JSON configuration string
   * @return an instance of {@link RowTrackingMetadataDomain}
   */
  public static RowTrackingMetadataDomain fromJsonConfiguration(String json) {
    return JsonMetadataDomain.fromJsonConfiguration(json, RowTrackingMetadataDomain.class);
  }

  /**
   * Creates an instance of {@link RowTrackingMetadataDomain} from a {@link SnapshotImpl}.
   *
   * @param snapshot the snapshot instance
   * @return an {@link Optional} containing the {@link RowTrackingMetadataDomain} if present
   */
  public static Optional<RowTrackingMetadataDomain> fromSnapshot(SnapshotImpl snapshot) {
    return JsonMetadataDomain.fromSnapshot(snapshot, RowTrackingMetadataDomain.class, DOMAIN_NAME);
  }

  public static final String DOMAIN_NAME = "delta.rowTracking";

  private long rowIdHighWatermark;

  /**
   * Constructs a RowTrackingMetadataDomain with the specified row ID high watermark.
   *
   * @param rowIdHighWatermark the row ID high watermark
   */
  @JsonCreator
  public RowTrackingMetadataDomain(@JsonProperty("rowIdHighWatermark") long rowIdHighWatermark) {
    this.rowIdHighWatermark = rowIdHighWatermark;
  }

  @Override
  public String getDomainName() {
    return DOMAIN_NAME;
  }

  public long getRowIdHighWatermark() {
    return rowIdHighWatermark;
  }

  public void setRowIdHighWatermark(long rowIdHighWatermark) {
    this.rowIdHighWatermark = rowIdHighWatermark;
  }
}
