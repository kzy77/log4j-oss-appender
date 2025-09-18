# Log4j2 OSS Appender

高性能 Log4j2 Appender，将应用日志实时上传到阿里云 OSS。

## 特性

- 不落盘：日志直接入内存队列并异步上传
- 无需改造：仅配置即可接入（log4j2.xml）
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

确保你的项目使用 Log4j2 并已引入 `log4j-core` 与 `log4j-api`。

## 配置（log4j2.xml）

```xml
<Configuration status="WARN">
  <Appenders>
    <OssAppender name="oss" endpoint="https://oss-cn-hangzhou.aliyuncs.com"
                 accessKeyId="${sys:OSS_AK}" accessKeySecret="${sys:OSS_SK}"
                 bucket="your-bucket" keyPrefix="app/demo"
                 queueCapacity="20000" batchMaxMessages="1000" batchMaxBytes="524288"
                 flushIntervalMs="2000" blockOnFull="true"
                 gzipEnabled="true" maxRetries="5" baseBackoffMs="500" maxBackoffMs="10000">
      <PatternLayout pattern="%d{ISO8601} %-5p %c{1.} - %m%ex{full}"/>
    </OssAppender>
    <Console name="console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{ISO8601} %-5p %c{1.} - %m%ex{full}"/>
    </Console>
  </Appenders>
  <Loggers>
    <Root level="info">
      <AppenderRef ref="oss"/>
      <AppenderRef ref="console"/>
    </Root>
  </Loggers>
 </Configuration>
```

> 推荐通过 JVM 参数传递密钥，避免硬编码：
> `-DOSS_AK=xxx -DOSS_SK=yyy`

## 参数说明

- endpoint：OSS 访问域名（带协议）
- accessKeyId / accessKeySecret：访问凭证（建议用 STS 临时密钥）
- bucket：目标 Bucket 名称
- keyPrefix：对象前缀，如 `app/demo`，会自动按 UTC 时间分层存储
- queueCapacity：队列最大条数，满时可阻塞或丢弃
- batchMaxMessages：单批最大条数
- batchMaxBytes：单批最大字节数（按日志序列化后计算）
- flushIntervalMs：定时刷新周期
- blockOnFull：队列满时是否阻塞生产者线程
- gzipEnabled：是否启用 gzip 压缩
- maxRetries：失败最大重试次数
- baseBackoffMs / maxBackoffMs：指数退避与最大等待

## 工作原理

日志事件经 Layout 序列化为字节，进入内存队列，由定时器和阈值触发聚合为批次，编码为 NDJSON（每行一条）。可选 gzip 压缩后，通过阿里云 OSS SDK 进行上传；失败时指数退避重试。

## 生产可用建议

- 建议将 `blockOnFull=true`，确保在突发流量时不丢日志
- 使用 STS 临时密钥+RAM 最小权限策略
- 选择就近地域的 `endpoint`，并开启内网/专线访问（如可用）
- keyPrefix 按业务/环境区分：`service/env/instance` 便于检索

## Cloudflare Pages 与 GitHub 导入（用于文档站点）

如果你计划将本仓库的 README/文档作为静态站点展示，可将仓库导入 Cloudflare Pages：

1. 将本仓库推送到 GitHub
2. 在 Cloudflare Pages 选择 “Connect to Git”，选择该仓库
3. Framework 选择 “None” 或使用 Docusaurus（如需文档站点）
4. Build command 留空或 `npm run build`（若你使用了文档框架）
5. Output directory 指向构建产物目录（如 `build`或`dist`）

此步骤与 Appender 的运行无关，仅用于托管文档页面。

## License

Apache-2.0

log4j oss appender
