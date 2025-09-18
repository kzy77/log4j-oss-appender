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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Log4j2 Appender：将日志事件序列化为 NDJSON，通过内存队列批处理并上传到 OSS。
 */
@Plugin(name = "OssAppender", category = "Core", elementType = "appender", printObject = true)
public class OssAppender extends AbstractAppender {

    private final BatchingQueue queue;
    private final OssUploader uploader;
    private final boolean blockOnFull;
    private final UploadHooks hooks;

    protected OssAppender(String name,
                          Filter filter,
                          Layout<? extends Serializable> layout,
                          boolean ignoreExceptions,
                          BatchingQueue queue,
                          OssUploader uploader,
                          boolean blockOnFull,
                          UploadHooks hooks) {
        super(name, filter, layout, ignoreExceptions);
        this.queue = queue;
        this.uploader = uploader;
        this.blockOnFull = blockOnFull;
        this.hooks = hooks;
        this.queue.start();
    }

    @Override
    public void append(LogEvent event) {
        byte[] bytes = getLayout().toByteArray(event);
        boolean accepted = queue.offer(bytes);
        if (!accepted && !blockOnFull) {
            // 队列满且未阻塞，触发丢弃回调
            hooks.onDropped(bytes.length, -1);
        }
    }

    @Override
    public void stop() {
        super.stop();
        try { queue.close(); } catch (Throwable ignore) {}
        try { uploader.close(); } catch (Throwable ignore) {}
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

            return new OssAppender(name, filter, effectiveLayout, true, batchingQueue, uploader, blockOnFull, hooks);
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


