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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.sql.DataSource;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.pdx.internal.PdxType;

@Experimental
public class CreateMappingPreconditionCheckFunction extends CliFunction<Object[]> {

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Object[]> context) {
    JdbcConnectorService service = FunctionContextArgumentProvider.getJdbcConnectorService(context);
    Object[] args = context.getArguments();
    RegionMapping regionMapping = (RegionMapping) args[0];
    String remoteInputStreamName = (String) args[1];
    RemoteInputStream remoteInputStream = (RemoteInputStream) args[2];

    String dataSourceName = regionMapping.getDataSourceName();
    DataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      throw new JdbcConnectorException("JDBC data-source named \"" + dataSourceName
          + "\" not found. Create it with gfsh 'create data-source --pooled --name="
          + dataSourceName + "'.");
    }

    TableMetaDataView tableMetaData = service.getTableMetaDataView(regionMapping);

    Class<?> pdxClazz =
        loadPdxClass(regionMapping.getPdxName(), remoteInputStreamName, remoteInputStream);
    PdxType pdxType = service.getPdxTypeForClass(context.getCache(), pdxClazz);

    List<FieldMapping> fieldMappings =
        service.createDefaultFieldMapping(regionMapping, pdxType, tableMetaData);
    Object[] output = new Object[2];
    output[1] = fieldMappings;
    if (regionMapping.getIds() == null || regionMapping.getIds().isEmpty()) {
      List<String> keyColumnNames = tableMetaData.getKeyColumnNames();
      // TODO 未設定ありでもOK？
      output[0] = String.join(",", keyColumnNames);
    }
    String member = context.getMemberName();
    return new CliFunctionResult(member, output);
  }

  private Class<?> loadPdxClass(String className, String remoteInputStreamName,
      RemoteInputStream remoteInputStream) {
    try {
      if (remoteInputStream != null) {
        return loadPdxClassFromRemoteStream(className, remoteInputStreamName, remoteInputStream);
      } else {
        return loadClass(className);
      }
    } catch (ClassNotFoundException ex) {
      throw new JdbcConnectorException(
          "The pdx class \"" + className + "\" could not be loaded because: " + ex);
    }
  }

  private Class<?> loadPdxClassFromRemoteStream(String className, String remoteInputStreamName,
      RemoteInputStream remoteInputStream) throws ClassNotFoundException {
    Path tempDir = createTemporaryDirectory("pdx-class-dir-");
    try {
      File file =
          copyRemoteInputStreamToTempFile(className, remoteInputStreamName, remoteInputStream,
              tempDir);
      return loadClass(className, createURL(file, tempDir));
    } finally {
      deleteDirectory(tempDir);
    }
  }

  Path createTemporaryDirectory(String prefix) {
    try {
      return createTempDirectory(prefix);
    } catch (IOException ex) {
      throw new JdbcConnectorException(
          "Could not create a temporary directory with the prefix \"" + prefix + "\" because: "
              + ex);
    }

  }

  void deleteDirectory(Path tempDir) {
    try {
      FileUtils.deleteDirectory(tempDir.toFile());
    } catch (IOException ioe) {
      // ignore
    }
  }

  private URL createURL(File file, Path tempDir) {
    URI uri;
    if (isJar(file.getName())) {
      uri = file.toURI();
    } else {
      uri = tempDir.toUri();
    }
    try {
      return uri.toURL();
    } catch (MalformedURLException e) {
      throw new JdbcConnectorException(
          "Could not convert \"" + uri + "\" to a URL, because: " + e);
    }
  }

  private boolean isJar(String fileName) {
    String fileExtension = FilenameUtils.getExtension(fileName);
    return fileExtension.equalsIgnoreCase("jar");
  }

  private File copyRemoteInputStreamToTempFile(String className, String remoteInputStreamName,
      RemoteInputStream remoteInputStream, Path tempDir) {
    if (!isJar(remoteInputStreamName) && className.contains(".")) {
      File packageDir = new File(tempDir.toFile(), className.replace(".", "/")).getParentFile();
      packageDir.mkdirs();
      tempDir = packageDir.toPath();
    }
    try {
      Path tempPdxClassFile = Paths.get(tempDir.toString(), remoteInputStreamName);
      try (InputStream input = RemoteInputStreamClient.wrap(remoteInputStream);
          FileOutputStream output = new FileOutputStream(tempPdxClassFile.toString())) {
        copyFile(input, output);
      }
      return tempPdxClassFile.toFile();
    } catch (IOException iox) {
      throw new JdbcConnectorException(
          "The pdx class file \"" + remoteInputStreamName
              + "\" could not be copied to a temporary file, because: " + iox);
    }
  }

  // unit test mocks this method
  DataSource getDataSource(String dataSourceName) {
    return JNDIInvoker.getDataSource(dataSourceName);
  }

  // unit test mocks this method
  Class<?> loadClass(String className) throws ClassNotFoundException {
    return ClassPathLoader.getLatest().forName(className);
  }

  // unit test mocks this method
  Class<?> loadClass(String className, URL url) throws ClassNotFoundException {
    return URLClassLoader.newInstance(new URL[] {url}).loadClass(className);
  }

  // unit test mocks this method
  Path createTempDirectory(String prefix) throws IOException {
    return Files.createTempDirectory(prefix);
  }

  // unit test mocks this method
  void copyFile(InputStream input, FileOutputStream output) throws IOException {
    IOUtils.copyLarge(input, output);
  }
}
