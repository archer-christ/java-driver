/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static com.datastax.oss.driver.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class RemoveNodeRefreshTest {

  private static final InetSocketAddress ADDRESS1 = new InetSocketAddress("127.0.0.1", 9042);
  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultConfigProfile;

  private DefaultNode node1;
  private DefaultNode node2;

  @Before
  public void setup() throws Exception {
    Mockito.when(context.config()).thenReturn(config);
    Mockito.when(config.getDefaultProfile()).thenReturn(defaultConfigProfile);

    node1 = new DefaultNode(ADDRESS1, context);
    node2 = new DefaultNode(ADDRESS2, context);
  }

  @Test
  public void should_remove_existing_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2), "test");
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(ADDRESS2, context, "test");

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1);
    assertThat(result.events).containsExactly(NodeStateEvent.removed(node2));
  }

  @Test
  public void should_not_remove_nonexistent_node() {
    // Given
    DefaultMetadata oldMetadata = new DefaultMetadata(ImmutableMap.of(ADDRESS1, node1), "test");
    RemoveNodeRefresh refresh = new RemoveNodeRefresh(ADDRESS2, context, "test");

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(ADDRESS1);
    assertThat(result.events).isEmpty();
  }
}
