package io.github.ossappender.log4j2;

import io.github.ossappender.core.BatchingQueue;
import io.github.ossappender.core.OssUploader;
import io.github.ossappender.core.UploadHooks;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Log4j2 Appender：将日志事件序列化为 NDJSON，通过内存队列批处理并上传到 OSS。
 */
@Plugin(name = "OssAppender", category = "Core", elementType = "appender", printObject = true)
public class OssAppender extends AbstractAppender {

    private static final Logger logger = LogManager.getLogger(OssAppender.class);

    private final BatchingQueue batchingQueue;
    private final io.github.ossappender.core.DisruptorBatchingQueue disruptorQueue;
    private final OssUploader uploader;
    private final boolean blockOnFull;
    private final UploadHooks hooks;

    protected OssAppender(String name,
                          Filter filter,
                          Layout<? extends Serializable> layout,
                          boolean ignoreExceptions,
                          BatchingQueue batchingQueue,
                          io.github.ossappender.core.DisruptorBatchingQueue disruptorQueue,
                          OssUploader uploader,
                          boolean blockOnFull,
                          UploadHooks hooks) {
        super(name, filter, layout, ignoreExceptions);
        this.batchingQueue = batchingQueue;
        this.disruptorQueue = disruptorQueue;
        this.uploader = uploader;
        this.blockOnFull = blockOnFull;
        this.hooks = hooks;
    }

    @Override
    public void append(LogEvent event) {
        try {
            byte[] bytes = getLayout().toByteArray(event);
            boolean accepted;
            if (batchingQueue != null) {
                accepted = batchingQueue.offer(bytes);
            } else {
                accepted = disruptorQueue.offer(bytes);
            }
            if (!accepted && !blockOnFull) {
                hooks.onDropped(bytes.length, -1);
            }
        } catch (Throwable throwable) {
            // Log the exception but don't throw it to avoid affecting business threads
            logger.warn("Failed to append log event to queue", throwable);
        }
    }

    @Override
    public void stop() {
        super.stop();
        try {
            if (batchingQueue != null) {
                batchingQueue.close();
            } else {
                disruptorQueue.close();
            }
        } catch (Throwable throwable) {
            logger.warn("Failed to close queue", throwable);
        }
        try { 
            uploader.close(); 
        } catch (Throwable throwable) {
            logger.warn("Failed to close uploader", throwable);
        }
    }

    /**
     * Builder：将 XML 配置映射为队列与上传器参数。
     */
    public static class Builder implements org.apache.logging.log4j.core.util.Builder<OssAppender> {
        @PluginAttribute("name")
        @Required
        private String name;

        // OSS config
        @PluginAttribute("endpoint") @Required private String endpoint;
        @PluginAttribute("accessKeyId") @Required private String accessKeyId;
        @PluginAttribute("accessKeySecret") @Required private String accessKeySecret;
        @PluginAttribute("bucket") @Required private String bucket;
        @PluginAttribute("keyPrefix") private String keyPrefix = "logs";

        // Queue & batching
        @PluginAttribute("queueCapacity") private int queueCapacity = 10000;
        @PluginAttribute("batchMaxMessages") private int batchMaxMessages = 1000;
        @PluginAttribute("batchMaxBytes") private int batchMaxBytes = 1024 * 512;
        @PluginAttribute("flushIntervalMs") private long flushIntervalMs = 2000;
        @PluginAttribute("blockOnFull") private boolean blockOnFull = true;
        @PluginAttribute("useDisruptor") private boolean useDisruptor = false;
        @PluginAttribute("multiProducer") private boolean multiProducer = true;

        // Retry & gzip
        @PluginAttribute("gzipEnabled") private boolean gzipEnabled = true;
        @PluginAttribute("maxRetries") private int maxRetries = 5;
        @PluginAttribute("baseBackoffMs") private long baseBackoffMs = 500;
        @PluginAttribute("maxBackoffMs") private long maxBackoffMs = 10_000;

        // Hooks
        @PluginAttribute("listenerClass") private String listenerClass;

        // Layout
        private Layout<? extends Serializable> layout;
        private Filter filter;

        public Builder setName(String name) { this.name = name; return this; }
        public Builder setLayout(Layout<? extends Serializable> layout) { this.layout = layout; return this; }
        public Builder setFilter(Filter filter) { this.filter = filter; return this; }

        @Override
        public OssAppender build() {
            Layout<? extends Serializable> effectiveLayout = layout != null ? layout : PatternLayout.newBuilder().withPattern("%m").withCharset(Charset.forName("UTF-8")).build();

            UploadHooks hooks = instantiateHooks(listenerClass);

            OssUploader uploader = new OssUploader(
                    endpoint, accessKeyId, accessKeySecret,
                    bucket, keyPrefix, gzipEnabled,
                    maxRetries, baseBackoffMs, maxBackoffMs,
                    hooks
            );

            if (useDisruptor) {
                io.github.ossappender.core.DisruptorBatchingQueue disruptorQueue = new io.github.ossappender.core.DisruptorBatchingQueue(
                        nearestPowerOfTwo(queueCapacity),
                        batchMaxMessages,
                        batchMaxBytes,
                        flushIntervalMs,
                        blockOnFull,
                        multiProducer,
                        (List<io.github.ossappender.core.BatchingQueue.LogEvent> events, int totalBytes) -> {
                            uploader.uploadBatch(events, totalBytes);
                            return true;
                        }
                );
                disruptorQueue.start();
                return new OssAppender(name, filter, effectiveLayout, true, null, disruptorQueue, uploader, blockOnFull, hooks);
            } else {
                BatchingQueue batchingQueue = new BatchingQueue(
                        queueCapacity,
                        batchMaxMessages,
                        batchMaxBytes,
                        flushIntervalMs,
                        blockOnFull,
                        (List<io.github.ossappender.core.BatchingQueue.LogEvent> events, int totalBytes) -> {
                            uploader.uploadBatch(events, totalBytes);
                            return true;
                        }
                );
                batchingQueue.start();
                return new OssAppender(name, filter, effectiveLayout, true, batchingQueue, null, uploader, blockOnFull, hooks);
            }
        }

        private int nearestPowerOfTwo(int n) {
            int x = 1;
            while (x < n) x <<= 1;
            return x;
        }

        /**
         * 通过反射构造钩子实现。
         */
        private UploadHooks instantiateHooks(String className) {
            if (className == null || className.trim().isEmpty()) {
                return UploadHooks.noop();
            }
            try {
                Class<?> c = Class.forName(className.trim());
                Object o = c.getDeclaredConstructor().newInstance();
                if (o instanceof UploadHooks) return (UploadHooks) o;
            } catch (Throwable ignore) {}
            return UploadHooks.noop();
        }
    }

    @PluginBuilderFactory
    public static Builder newBuilder() { return new Builder(); }
}


