package com.huawei.cse.porter.gateway;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.servicecomb.edge.core.AbstractEdgeDispatcher;
import org.apache.servicecomb.foundation.common.cache.VersionedCache;
import org.apache.servicecomb.foundation.common.concurrent.ConcurrentHashMapEx;
import org.apache.servicecomb.foundation.vertx.VertxUtils;
import org.apache.servicecomb.serviceregistry.RegistryUtils;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstance;
import org.apache.servicecomb.serviceregistry.definition.DefinitionConst;
import org.apache.servicecomb.serviceregistry.discovery.DiscoveryContext;
import org.apache.servicecomb.serviceregistry.discovery.DiscoveryTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class UiDispatcher extends AbstractEdgeDispatcher {
    private static Logger LOGGER = LoggerFactory.getLogger(UiDispatcher.class);

    private static Vertx vertx = VertxUtils.getOrCreateVertxByName("transport", null);

    private static HttpClient httpClient = vertx.createHttpClient(new HttpClientOptions());

    private Map<String, DiscoveryTree> discoveryTrees = new ConcurrentHashMapEx<>();

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int getOrder() {
        return 10001;
    }

    @Override
    public void init(Router router) {
        String regex = "/ui/([^\\/]+)/(.*)";
        router.routeWithRegex(regex).failureHandler(this::onFailure).handler(this::onRequest);
    }

    protected void onRequest(RoutingContext context) {
        Map<String, String> pathParams = context.pathParams();

        String microserviceName = pathParams.get("param0");
        String path = "/" + pathParams.get("param1");

        URI uri = chooseServer(microserviceName);

        if (uri == null) {
            context.response().setStatusCode(404);
            context.response().end();
            return;
        }

        // 使用HttpClient转发请求
        HttpClientRequest clietRequest =
            httpClient.request(context.request().method(),
                    uri.getPort(),
                    uri.getHost(),
                    "/" + path,
                    clientResponse -> {
                        context.request().response().setChunked(true);
                        context.request().response().setStatusCode(clientResponse.statusCode());
                        context.request().response().headers().setAll(clientResponse.headers());
                        clientResponse.handler(data -> {
                            context.request().response().write(data);
                        });
                        clientResponse.endHandler((v) -> context.request().response().end());
                    });
        clietRequest.setChunked(true);
        clietRequest.headers().setAll(context.request().headers());
        context.request().handler(data -> {
            clietRequest.write(data);
        });
        context.request().endHandler((v) -> clietRequest.end());
    }

    private URI chooseServer(String serviceName) {
        URI uri = null;

        DiscoveryContext context = new DiscoveryContext();
        context.setInputParameters(serviceName);
        DiscoveryTree discoveryTree = discoveryTrees.computeIfAbsent(serviceName, key -> {
            return new DiscoveryTree();
        });
        VersionedCache serversVersionedCache = discoveryTree.discovery(context,
                RegistryUtils.getAppId(),
                serviceName,
                DefinitionConst.VERSION_RULE_ALL);
        Map<String, MicroserviceInstance> servers = serversVersionedCache.data();
        String[] endpoints = asArray(servers);
        if (endpoints.length > 0) {
            int index = Math.abs(counter.getAndIncrement() % endpoints.length);
            String endpoint = endpoints[index];
            try {
                uri = new URI(endpoint);
            } catch (URISyntaxException e) {
                LOGGER.error("", e);
            }
        }
        return uri;
    }

    private String[] asArray(Map<String, MicroserviceInstance> servers) {
        List<String> endpoints = new LinkedList<>();
        for (MicroserviceInstance instance : servers.values()) {
            endpoints.addAll(instance.getEndpoints());
        }
        return endpoints.toArray(new String[endpoints.size()]);
    }
}
