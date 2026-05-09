FROM alpine:3.20 AS runtime

ARG TARGETARCH

RUN apk add --no-cache ca-certificates su-exec \
    && adduser -D -H -u 10001 easy \
    && mkdir -p /app/data /app/logs \
    && chown -R easy:easy /app

WORKDIR /app

# CI 预先构建好多架构二进制后放入 release/linux/<arch>/proxyweave
# Docker 镜像只做运行时封装，不在镜像内编译 Go。
COPY release/linux/${TARGETARCH}/proxyweave /usr/local/bin/proxyweave
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/proxyweave /usr/local/bin/entrypoint.sh

# 固定入口端口：Pool/Hybrid 2323, Management 9091
# 多端口模式端口范围按运行配置决定，建议在 compose / run 时显式映射。
EXPOSE 2323 9091

VOLUME ["/app/data"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
