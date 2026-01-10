# Dockerfile for NestJS API
FROM node:18-bullseye

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci

# Install tini for proper signal handling
RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*

# Ensure that all files are owned by the node user to avoid permissions issues
RUN chown -R node:node /app

# Switch to a non-root user for safety (Node.js user)
USER node

# Copy the rest of the application source code with correct ownership
COPY --chown=node:node . .

# Ensure that all binaries in node_modules/.bin are executable
RUN chmod -R +x /app/node_modules/.bin

# Build TypeScript
RUN npm run build

# Legacy entrypoint shim for Compose expecting api/server.js
RUN printf "require('../dist/api/main.js');\n" > /app/api/server.js

# Use tini to handle signals and run the app
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["node", "dist/api/main.js"]
