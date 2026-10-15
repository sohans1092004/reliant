# One image for every Reliant process - they share one codebase, so each Kubernetes
# Deployment just runs a different command:
#   docker run reliant node consumer.js
FROM node:22.17.0-bookworm-slim

ENV NODE_ENV=production
WORKDIR /app

# Dependencies first, so code changes don't invalidate the npm layer.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY core ./core
COPY lib ./lib
COPY public ./public
COPY *.js schema.sql ./

# The base image ships an unprivileged "node" user.
USER node

# Ingest by default; Kubernetes overrides this per Deployment.
CMD ["node", "ingest-api.js"]
