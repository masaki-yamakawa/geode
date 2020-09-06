/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc.internal.cli;

import java.util.List;

import com.healthmarketscience.rmiio.RemoteInputStream;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;

@Experimental
public class CreateMappingPreconditionCheckFunction extends CliFunction<Object[]> {

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    Object[] args = context.getArguments();
    RegionMapping regionMapping = (RegionMapping) args[0];
    String remoteInputStreamName = (String) args[1];
    RemoteInputStream remoteInputStream = (RemoteInputStream) args[2];

    List<FieldMapping> fieldMappings = service.createDefaultFieldMapping(regionMapping,
        context.getCache(), remoteInputStreamName, remoteInputStream);
    Object[] output = new Object[2];
    output[1] = fieldMappings;
    if (regionMapping.getIds() == null || regionMapping.getIds().isEmpty()) {
      TableMetaDataView tableMetaData = service.getTableMetaDataView(regionMapping);
      List<String> keyColumnNames = tableMetaData.getKeyColumnNames();
      output[0] = String.join(",", keyColumnNames);
    }
    String member = context.getMemberName();
    return new CliFunctionResult(member, output);
  }
}
