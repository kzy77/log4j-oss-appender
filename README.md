# Log4j OSS Appender

高性能 Log4j Appender，将应用日志实时上传到阿里云 OSS。

## 特性

- 不落盘：日志直接入内存队列并异步上传
- 无需改造：仅配置即可接入（log4j.xml）
- 异步高吞吐：批处理、gzip 压缩、HTTP 连接复用
- 不丢日志：可配置为生产侧阻塞等待，提供上传回调监听
- 可配置：队列大小、批量条数/字节、刷新间隔、重试与退避、gzip 等

## 安装

引入依赖（Maven）：
```xml
<dependency>
  <groupId>io.github.log4j-oss-appender</groupId>
  <artifactId>log4j-oss-appender</artifactId>
  <version>0.1.0</version>
  <scope>runtime</scope>
</dependency>
```

确保你的项目使用 Log4j 并已引入 `log4j-core` 与 `log4j-api`。

## 配置（log4j.xml）
```xml
<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">
  <appender name="oss" class="io.github.log4j.oss.OssAppender">
    <param name="endpoint" value="https://oss-cn-hangzhou.aliyuncs.com"/>
    <param name="accessKeyId" value="${sys:OSS_AK}"/>
    <param name="accessKeySecret" value="${sys:OSS_SK}"/>
    <param name="bucket" value="your-bucket"/>
    <param name="keyPrefix" value="app/demo"/>
    <param name="queueCapacity" value="20000"/>
    <param name="batchMaxMessages" value="1000"/>
    <param name="batchMaxBytes" value="524288"/>
    <param name="flushIntervalMs" value="2000"/>
    <param name="blockOnFull" value="true"/>
    <param name="gzipEnabled" value="true"/>
    <param name="maxRetries" value="5"/>
    <param name="baseBackoffMs" value="500"/>
    <param name="maxBackoffMs" value="10000"/>
    <layout class="org.apache.log4j.PatternLayout">
      <param name="ConversionPattern" value="%d{ISO8601} %-5p %c{1.} - %m%ex{full}"/>
    </layout>
  </appender>
  <root>
    <priority value="info"/>
    <appender-ref ref="oss"/>
  </root>
</log4j:configuration>
```

> 推荐通过 JVM 参数传递密钥，避免硬编码：`-DOSS_AK=xxx -DOSS_SK=yyy`

## 参数说明

- `endpoint`：OSS 访问域名（带协议）
- `accessKeyId` / `accessKeySecret`：访问凭证（建议用 STS 临时密钥）
- `bucket`：目标 Bucket 名称
- `keyPrefix`：对象前缀，如 `app/demo`，会自动按 UTC 时间分层存储
- `queueCapacity`：队列最大条数，满时可阻塞或丢弃
- `batchMaxMessages`：单批最大条数
- `batchMaxBytes`：单批最大字节数（按日志序列化后计算）
- `flushIntervalMs`：定时刷新周期
- `blockOnFull`：队列满时是否阻塞生产者线程
- `gzipEnabled`：是否启用 gzip 压缩
- `maxRetries`：失败最大重试次数
- `baseBackoffMs` / `maxBackoffMs`：指数退避与最大等待

## 工作原理

日志事件经 Layout 序列化为字节，进入内存队列，由定时器和阈值触发聚合为批次，编码为 NDJSON（每行一条）。可选 gzip 压缩后，通过阿里云 OSS SDK 进行上传；失败时指数退避重试。

## 生产可用建议

- 建议将 `blockOnFull=true`，确保在突发流量时不丢日志
- 使用 STS 临时密钥+RAM 最小权限策略
- 选择就近地域的 `endpoint`，并开启内网/专线访问（如可用）
- `keyPrefix` 按业务/环境区分：`service/env/instance` 便于检索

## 许可证

Apache-2.0