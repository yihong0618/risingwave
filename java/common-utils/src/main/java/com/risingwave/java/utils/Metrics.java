package com.risingwave.java.utils;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class Metrics {
    private static volatile PrometheusMeterRegistry registry = null;

    public static MeterRegistry getRegistry() {
        if (registry == null) {
            synchronized (Metrics.class) {
                if (registry == null) {
                    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
                    try {
                        HttpServer server = HttpServer.create(new InetSocketAddress(50053), 0);
                        server.createContext(
                                "/prometheus",
                                httpExchange -> {
                                    String response = registry.scrape();
                                    httpExchange.sendResponseHeaders(
                                            200, response.getBytes().length);
                                    try (OutputStream os = httpExchange.getResponseBody()) {
                                        os.write(response.getBytes());
                                    }
                                });

                        new Thread(server::start).start();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return registry;
    }
}
