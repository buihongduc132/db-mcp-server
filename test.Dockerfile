FROM debian:bullseye-slim

ENV DEBIAN_FRONTEND=noninteractive \
    GLAMA_VERSION="0.2.0" \
    PATH="/home/service-user/.local/bin:${PATH}"

RUN (groupadd -r service-user) && (useradd -u 1987 -r -m -g service-user service-user) && (mkdir -p /home/service-user/.local/bin /app) && (chown -R service-user:service-user /home/service-user /app) && (apt-get update) && (apt-get install -y --no-install-recommends build-essential curl wget software-properties-common libssl-dev zlib1g-dev git) && (rm -rf /var/lib/apt/lists/*) && (curl -fsSL https://deb.nodesource.com/setup_22.x | bash -) && (apt-get install -y nodejs) && (apt-get clean) && (npm install -g mcp-proxy@2.10.6) && (npm install -g pnpm@9.15.5) && (npm install -g bun@1.1.42) && (node --version) && (curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR="/usr/local/bin" sh) && (uv python install 3.13 --default --preview) && (ln -s $(uv python find) /usr/local/bin/python) && (python --version) && (apt-get clean) && (rm -rf /var/lib/apt/lists/*) && (rm -rf /tmp/*) && (rm -rf /var/tmp/*) && (su - service-user -c "uv python install 3.13 --default --preview && python --version")

USER service-user

WORKDIR /app

# RUN git clone https://github.com/FreePeak/db-mcp-server . && git checkout 01bc85c8c93dbea90c1a2be729d7fd71b4d40f47
COPY . .

# Add -no-log flag to prevent non-JSON log messages from causing parse errors in mcp-proxy
# This fixes errors like: SyntaxError: Unexpected token 'N', "No active "... is not valid JSON
# Redirect stderr to /dev/null to ensure no log messages interfere with JSON-RPC communication
# CMD ["sh", "-c", "exec mcp-proxy /app/server-linux -t stdio -no-log --stdio 2>/dev/null"]

CMD ["mcp-proxy", "/app/server-linux", "-t", "stdio", "-no-log", "--stdio"]