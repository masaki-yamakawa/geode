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
package org.apache.geode.connectors.jdbc.internal.xml;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxOutputStream;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PdxWriterImpl;
import org.apache.geode.pdx.internal.TypeRegistry;

public class RegionMappingConfiguration implements Extension<Region<?, ?>> {

  private final RegionMapping mapping;

  public RegionMappingConfiguration(RegionMapping mapping) {
    this.mapping = mapping;
  }

  @Override
  public XmlGenerator<Region<?, ?>> getXmlGenerator() {
    return null;
  }

  @Override
  public void beforeCreate(Extensible<Region<?, ?>> source, Cache cache) {
    // nothing
  }

  @Override
  public void onCreate(Extensible<Region<?, ?>> source, Extensible<Region<?, ?>> target) {
    final ExtensionPoint<Region<?, ?>> extensionPoint = target.getExtensionPoint();
    final Region<?, ?> region = extensionPoint.getTarget();
    InternalCache internalCache = (InternalCache) region.getRegionService();
    JdbcConnectorService service = internalCache.getService(JdbcConnectorService.class);
    if (mapping.getFieldMappings().isEmpty()) {
      createDefaultFieldMapping(internalCache);
    }
    service.validateMapping(mapping);
    createRegionMapping(service, mapping);
  }

  // from CreateMappingPreconditionCheckFunction
  private void createDefaultFieldMapping(InternalCache internalCache) {
    String dataSourceName = mapping.getDataSourceName();

    DataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      throw new JdbcConnectorException("JDBC data-source named \"" + dataSourceName
          + "\" not found. Create it with gfsh 'create data-source --pooled --name="
          + dataSourceName + "'.");
    }
    TypeRegistry typeRegistry = internalCache.getPdxRegistry();
    PdxType pdxType = getPdxTypeForClass(internalCache, typeRegistry, mapping.getPdxName());
    try (Connection connection = dataSource.getConnection()) {
      TableMetaDataView tableMetaData =
          getTableMetaDataManager().getTableMetaDataView(connection, mapping);
      // TODO the table name returned in tableMetaData may be different than
      // the table name specified on the command line at this point.
      // Do we want to update the region mapping to hold the "real" table name
      // Object[] output = new Object[2];
      ArrayList<FieldMapping> fieldMappings = new ArrayList<>();
      Set<String> columnNames = tableMetaData.getColumnNames();
      List<PdxField> pdxFields = pdxType.getFields();
      for (String jdbcName : columnNames) {
        boolean isNullable = tableMetaData.isColumnNullable(jdbcName);
        JDBCType jdbcType = tableMetaData.getColumnDataType(jdbcName);
        FieldMapping fieldMapping =
            createFieldMapping(jdbcName, jdbcType.getName(), isNullable, pdxFields);
        mapping.addFieldMapping(fieldMapping);
      }
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
  }

  private void createRegionMapping(JdbcConnectorService service,
      RegionMapping regionMapping) {
    try {
      service.createRegionMapping(regionMapping);
    } catch (RegionMappingExistsException e) {
      throw new InternalGemFireException(e);
    }
  }

  // from JdbcConnectorServiceImpl
  DataSource getDataSource(String dataSourceName) {
    return JNDIInvoker.getDataSource(dataSourceName);
  }

  // from CreateMappingPreconditionCheckFunction
  TableMetaDataManager getTableMetaDataManager() {
    return new TableMetaDataManager();
  }

  // from CreateMappingPreconditionCheckFunction
  private PdxType getPdxTypeForClass(InternalCache cache, TypeRegistry typeRegistry,
      String className) {
    try {
      Class<?> clazz = loadClass(className);
      // TypeRegistry.getAutoSerializableManager().getClassInfo(clazz);
      PdxType result = typeRegistry.getExistingTypeForClass(clazz);
      if (result != null) {
        return result;
      }
      return generatePdxTypeForClass(cache, typeRegistry, clazz);
    } catch (ClassNotFoundException ex) {
      throw new JdbcConnectorException(
          "The pdx class \"" + className + "\" could not be loaded because: " + ex);
    }
  }

  // from CreateMappingPreconditionCheckFunction
  private PdxType generatePdxTypeForClass(InternalCache cache, TypeRegistry typeRegistry,
      Class<?> clazz) {
    Object object = createInstance(clazz);
    try {
      cache.registerPdxMetaData(object);
    } catch (SerializationException ex) {
      String className = clazz.getName();
      ReflectionBasedAutoSerializer serializer =
          getReflectionBasedAutoSerializer("\\Q" + className + "\\E");
      PdxWriter writer = createPdxWriter(typeRegistry, object);
      boolean result = serializer.toData(object, writer);
      if (!result) {
        throw new JdbcConnectorException(
            "Could not generate a PdxType using the ReflectionBasedAutoSerializer for the class  "
                + clazz.getName() + " after failing to register pdx metadata due to "
                + ex.getMessage() + ". Check the server log for details.");
      }
    }
    // serialization will leave the type in the registry
    return typeRegistry.getExistingTypeForClass(clazz);
  }

  // from CreateMappingPreconditionCheckFunction
  PdxWriter createPdxWriter(TypeRegistry typeRegistry, Object object) {
    return new PdxWriterImpl(typeRegistry, object, new PdxOutputStream());
  }

  // from CreateMappingPreconditionCheckFunction
  ReflectionBasedAutoSerializer getReflectionBasedAutoSerializer(String className) {
    return new ReflectionBasedAutoSerializer(className);
  }

  // from CreateMappingPreconditionCheckFunction
  private Object createInstance(Class<?> clazz) {
    try {
      Constructor<?> ctor = clazz.getConstructor();
      return ctor.newInstance(new Object[] {});
    } catch (NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
      throw new JdbcConnectorException(
          "Could not generate a PdxType for the class " + clazz.getName()
              + " because it did not have a public zero arg constructor. Details: " + ex);
    }
  }

  // from CreateMappingPreconditionCheckFunction
  Class<?> loadClass(String className) throws ClassNotFoundException {
    return ClassPathLoader.getLatest().forName(className);
  }

  // from CreateMappingPreconditionCheckFunction
  private FieldMapping createFieldMapping(String jdbcName, String jdbcType, boolean jdbcNullable,
      List<PdxField> pdxFields) {
    String pdxName = null;
    String pdxType = null;
    for (PdxField pdxField : pdxFields) {
      if (pdxField.getFieldName().equals(jdbcName)) {
        pdxName = pdxField.getFieldName();
        pdxType = pdxField.getFieldType().name();
        break;
      }
    }
    if (pdxName == null) {
      // look for one inexact match
      for (PdxField pdxField : pdxFields) {
        if (pdxField.getFieldName().equalsIgnoreCase(jdbcName)) {
          if (pdxName != null) {
            throw new JdbcConnectorException(
                "More than one PDX field name matched the column name \"" + jdbcName + "\"");
          }
          pdxName = pdxField.getFieldName();
          pdxType = pdxField.getFieldType().name();
        }
      }
    }
    if (pdxName == null) {
      throw new JdbcConnectorException(
          "No PDX field name matched the column name \"" + jdbcName + "\"");
    }
    return new FieldMapping(pdxName, pdxType, jdbcName, jdbcType, jdbcNullable);
  }
}
