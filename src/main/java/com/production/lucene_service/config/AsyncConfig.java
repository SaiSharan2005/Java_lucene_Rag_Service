package com.production.lucene_service.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@Slf4j
public class AsyncConfig {

    @Bean(name = "ingestionExecutor")
    public Executor ingestionExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(10);
        executor.setThreadNamePrefix("ingest-job-");
        executor.setRejectedExecutionHandler((runnable, pool) ->
                log.warn("Ingestion job rejected - queue full. Max concurrent: 2, queue: 10"));
        executor.initialize();
        log.info("Initialized ingestion executor: corePool=2, maxPool=2, queue=10");
        return executor;
    }
}
