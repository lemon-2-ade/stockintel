# syntax=docker/dockerfile:1.7
# Dashboard: build the Vite SPA, serve it with nginx, which also proxies
# /api and /ws to the API so the browser talks to a single origin.
ARG NODE_IMAGE=node:22.20-bookworm-slim
ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:1.29-alpine

FROM ${NODE_IMAGE} AS build
WORKDIR /app
COPY apps/frontend/package.json apps/frontend/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY apps/frontend/ ./
RUN npm run build

FROM ${NGINX_IMAGE} AS runtime
COPY infrastructure/docker/frontend-nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
