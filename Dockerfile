# syntax=docker/dockerfile:1
ARG NODE_IMAGE=node:24-alpine

FROM ${NODE_IMAGE} AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM deps AS build-web
WORKDIR /app
COPY vite.config.ts ./
COPY types ./types
COPY web ./web
RUN npm run build:web

FROM deps AS build-server
WORKDIR /app
COPY tsconfig.json tsconfig.build.json ./
COPY types ./types
COPY src ./src
RUN npm run build:server

FROM ${NODE_IMAGE} AS runtime
ARG APP_VERSION=dev
ENV NODE_ENV=production \
    APP_VERSION=${APP_VERSION} \
    PORT=8080 \
    HOST=0.0.0.0 \
    CONFIG_DIR=/config \
    SPOOL_DIR=/spool \
    LOGS_ROOT=/logs \
    WEB_ROOT=/app/web/dist \
    LOG_LEVEL=info
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --from=build-server /app/dist ./dist
COPY --from=build-web /app/web/dist ./web/dist

RUN addgroup -g 10001 -S app \
 && adduser -u 10001 -S app -G app \
 && mkdir -p /config /spool /logs \
 && chown -R 10001:10001 /config /spool /logs

USER 10001:10001
EXPOSE 8080
VOLUME ["/config", "/spool", "/logs"]

# busybox wget; alpine has no curl.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1

CMD ["node", "dist/src/index.js"]
