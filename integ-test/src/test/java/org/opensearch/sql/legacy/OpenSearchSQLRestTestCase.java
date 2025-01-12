/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import static java.util.Collections.unmodifiableList;

/**
 * OpenSearch SQL integration test base class to support both security disabled and enabled OpenSearch cluster.
 * Allows interaction with multiple external test clusters using OpenSearch's {@link RestClient}.
 */
public abstract class OpenSearchSQLRestTestCase extends OpenSearchRestTestCase {

  private static final Logger LOG = LogManager.getLogger();
  public static final String REMOTE_CLUSTER = "remoteCluster";
  public static final String MATCH_ALL_REMOTE_CLUSTER = "*";

  private static RestClient remoteClient;
  /**
   * A client for the running remote OpenSearch cluster configured to take test administrative actions
   * like remove all indexes after the test completes
   */
  private static RestClient remoteAdminClient;

  protected boolean isHttps() {
    boolean isHttps = Optional.ofNullable(System.getProperty("https"))
        .map("true"::equalsIgnoreCase).orElse(false);
    if (isHttps) {
      //currently only external cluster is supported for security enabled testing
      if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
        throw new RuntimeException(
            "external cluster url should be provided for security enabled testing");
      }
    }

    return isHttps;
  }

  protected String getProtocol() {
    return isHttps() ? "https" : "http";
  }

  /**
   * Get the client to remote cluster used for ordinary api calls while writing a test.
   */
  protected static RestClient remoteClient() {
    return remoteClient;
  }

  /**
   * Get the client to remote cluster used for test administrative actions.
   * Do not use this while writing a test. Only use it for cleaning up after tests.
   */
  protected static RestClient remoteAdminClient() {
    return remoteAdminClient;
  }

  protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
    RestClientBuilder builder = RestClient.builder(hosts);
    if (isHttps()) {
      configureHttpsClient(builder, settings, hosts[0]);
    } else {
      configureClient(builder, settings);
    }

    builder.setStrictDeprecationMode(false);
    return builder.build();
  }

  // Modified from initClient in OpenSearchRestTestCase
  public void initRemoteClient() throws IOException {
    if (remoteClient == null) {
      assert remoteAdminClient == null;
      String cluster = getTestRestCluster(REMOTE_CLUSTER);
      String[] stringUrls = cluster.split(",");
      List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
      for (String stringUrl : stringUrls) {
        int portSeparator = stringUrl.lastIndexOf(':');
        if (portSeparator < 0) {
          throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
        }
        String host = stringUrl.substring(0, portSeparator);
        int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
        hosts.add(buildHttpHost(host, port));
      }
      final List<HttpHost> clusterHosts = unmodifiableList(hosts);
      remoteClient = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
      remoteAdminClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[0]));
    }
    assert remoteClient != null;
    assert remoteAdminClient != null;
  }

  /**
   * Get a comma delimited list of [host:port] to which to send REST requests.
   */
  protected String getTestRestCluster(String clusterName) {
    String cluster = System.getProperty("tests.rest." + clusterName + ".http_hosts");
    if (cluster == null) {
      throw new RuntimeException(
          "Must specify [tests.rest."
              + clusterName
              + ".http_hosts] system property with a comma delimited list of [host:port] "
              + "to which to send REST requests"
      );
    }
    return cluster;
  }

  /**
   * Get a comma delimited list of [host:port] for connections between clusters.
   */
  protected String getTestTransportCluster(String clusterName) {
    String cluster = System.getProperty("tests.rest." + clusterName + ".transport_hosts");
    if (cluster == null) {
      throw new RuntimeException(
          "Must specify [tests.rest."
              + clusterName
              + ".transport_hosts] system property with a comma delimited list of [host:port] "
              + "for connections between clusters"
      );
    }
    return cluster;
  }

  @AfterClass
  public static void closeRemoteClients() throws IOException {
    try {
      IOUtils.close(remoteClient, remoteAdminClient);
    } finally {
      remoteClient = null;
      remoteAdminClient = null;
    }
  }

  protected static void wipeAllOpenSearchIndices() throws IOException {
    wipeAllOpenSearchIndices(client());
    if (remoteClient() != null) {
      wipeAllOpenSearchIndices(remoteClient());
    }
  }

  protected static void wipeAllOpenSearchIndices(RestClient client) throws IOException {
    // include all the indices, included hidden indices.
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html#cat-indices-api-query-params
    try {
      Response response = client.performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
      JSONArray jsonArray = new JSONArray(EntityUtils.toString(response.getEntity(), "UTF-8"));
      for (Object object : jsonArray) {
        JSONObject jsonObject = (JSONObject) object;
        String indexName = jsonObject.getString("index");
        try {
          // System index, mostly named .opensearch-xxx or .opendistro-xxx, are not allowed to delete
          if (!indexName.startsWith(".opensearch") && !indexName.startsWith(".opendistro")) {
            client.performRequest(new Request("DELETE", "/" + indexName));
          }
        } catch (Exception e) {
          // TODO: Ignore index delete error for now. Remove this if strict check on system index added above.
          LOG.warn("Failed to delete index: " + indexName, e);
        }
      }
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  protected static void configureHttpsClient(RestClientBuilder builder, Settings settings,
                                             HttpHost httpHost)
      throws IOException {
    Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
    Header[] defaultHeaders = new Header[headers.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
    }
    builder.setDefaultHeaders(defaultHeaders);
    builder.setHttpClientConfigCallback(httpClientBuilder -> {
      String userName = Optional.ofNullable(System.getProperty("user"))
          .orElseThrow(() -> new RuntimeException("user name is missing"));
      String password = Optional.ofNullable(System.getProperty("password"))
          .orElseThrow(() -> new RuntimeException("password is missing"));
      BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider
          .setCredentials(new AuthScope(httpHost), new UsernamePasswordCredentials(userName,
              password.toCharArray()));
      try {
        final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
            .setSslContext(SSLContextBuilder.create()
                .loadTrustMaterial(null, (chains, authType) -> true)
                .build())
            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();

        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            .setConnectionManager(PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(tlsStrategy)
                .build());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
    final TimeValue socketTimeout =
        TimeValue.parseTimeValue(socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT);
    builder.setRequestConfigCallback(
        conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis()))));
    if (settings.hasValue(CLIENT_PATH_PREFIX)) {
      builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
    }
  }

  /**
   * Initialize rest client to remote cluster,
   * and create a connection to it from the coordinating cluster.
   */
  public void configureMultiClusters() throws IOException {
    initRemoteClient();

    Request connectionRequest = new Request("PUT", "_cluster/settings");
    String connectionSetting = "{\"persistent\": {\"cluster\": {\"remote\": {\""
        + REMOTE_CLUSTER
        + "\": {\"seeds\": [\""
        + getTestTransportCluster(REMOTE_CLUSTER).split(",")[0]
        + "\"]}}}}}";
    connectionRequest.setJsonEntity(connectionSetting);
    adminClient().performRequest(connectionRequest);
  }
}
