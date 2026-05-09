FROM debian:bookworm-slim AS runtime

ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates gosu \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 easy \
    && mkdir -p /etc/proxyweave \
    && chown -R easy:easy /etc/proxyweave

WORKDIR /app

# CI 预先构建好多架构二进制后放入 release/linux/<arch>/proxyweave
# Docker 镜像只做运行时封装，不在镜像内编译 Go。
COPY release/linux/ /tmp/release/linux/
RUN set -eux; \
    cp "/tmp/release/linux/${TARGETARCH}/proxyweave" /usr/local/bin/proxyweave; \
    chmod 0755 /usr/local/bin/proxyweave; \
    rm -rf /tmp/release

COPY --chown=easy:easy config.example.yaml /etc/proxyweave/config.yaml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# 固定入口端口：Pool/Hybrid 2323, Management 9091
# 多端口模式端口范围按运行配置决定，建议在 compose / run 时显式映射。
EXPOSE 2323 9091

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["--config", "/etc/proxyweave/config.yaml"]
