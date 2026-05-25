#!/usr/bin/env bash
set -euo pipefail

# deploy.sh - prepare data directory for Umbrel/Portainer and optionally run docker compose
# Usage:
#   ./deploy.sh [--data-dir DIR] [--uid UID] [--gid GID] [--deploy] [--compose-cmd "docker compose"]
# Examples:
#   ./deploy.sh --data-dir /home/umbrel/dealscout-data
#   sudo ./deploy.sh --data-dir /home/umbrel/dealscout-data --deploy

DEFAULT_DATA_DIR="${DEALSCOUT_DATA_DIR:-$HOME/dealscout-data}"
DATA_DIR="$DEFAULT_DATA_DIR"
UID_ARG=""
GID_ARG=""
DO_DEPLOY=0
COMPOSE_CMD="docker compose"
SESSION_BASENAME="${DEALSCOUT_SESSION:-dealscout_session}"

print_usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --data-dir DIR       Data directory on host (default: $DEFAULT_DATA_DIR)
  --uid UID            Set owner UID for data dir (default: current or SUDO_UID)
  --gid GID            Set owner GID for data dir (default: current or SUDO_GID)
  --deploy             Run "${COMPOSE_CMD} up -d --build" after preparing data
  --compose-cmd CMD    Compose command to run (default: "docker compose")
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir)
      DATA_DIR="$2"; shift 2;;
    --uid)
      UID_ARG="$2"; shift 2;;
    --gid)
      GID_ARG="$2"; shift 2;;
    --deploy)
      DO_DEPLOY=1; shift;;
    --compose-cmd)
      COMPOSE_CMD="$2"; shift 2;;
    -h|--help)
      print_usage; exit 0;;
    *)
      echo "Unknown arg: $1" >&2; print_usage; exit 1;;
  esac
done

echo "Preparing data directory: $DATA_DIR"
mkdir -p "$DATA_DIR"

# Copy a local authenticated Telethon session into the data dir if present.
SESSION_SOURCE="${SESSION_BASENAME}.session"
SESSION_TARGET="$DATA_DIR/${SESSION_BASENAME}.session"
if [ -f "$SESSION_TARGET" ]; then
  echo "Found existing $SESSION_TARGET; leaving as-is"
elif [ -f "$SESSION_SOURCE" ]; then
  cp "$SESSION_SOURCE" "$SESSION_TARGET"
  echo "Copied local Telegram session to $SESSION_TARGET"
else
  echo "No local Telegram session found at $SESSION_SOURCE; container will need a pre-authenticated session file"
fi

# Copy channels.json.example to data dir if channels.json is not present
EXAMPLE_SRC="listener/channels.json.example"
TARGET_CONF="$DATA_DIR/channels.json"
if [ -f "$TARGET_CONF" ]; then
  echo "Found existing $TARGET_CONF; leaving as-is"
else
  if [ -f "$EXAMPLE_SRC" ]; then
    cp "$EXAMPLE_SRC" "$TARGET_CONF"
    echo "Copied example channels.json to $TARGET_CONF"
  else
    echo "Warning: example channels.json not found at $EXAMPLE_SRC" >&2
  fi
fi

# Create a .env.example at project root if missing
ENV_EXAMPLE=".env.example"
if [ -f "$ENV_EXAMPLE" ]; then
  echo "$ENV_EXAMPLE already exists"
else
  cat > "$ENV_EXAMPLE" <<EOF
# Copy to .env and fill in values. Example:
TG_API_ID=12345
TG_API_HASH=your_api_hash_here
TG_PHONE=+551199999999
DEALSCOUT_DATA_DIR=$DATA_DIR
DEALSCOUT_ENABLE_WEBHOOK=false
DEALSCOUT_VERBOSE_STARTUP=true
DEALSCOUT_AUTO_RESTART=true
DEALSCOUT_MAX_RESTARTS=0
DEALSCOUT_RESTART_DELAY_SECONDS=2
DEALSCOUT_MAX_RESTART_DELAY_SECONDS=300
EOF
  echo "Created $ENV_EXAMPLE (copy to .env and edit values)"
fi

# Adjust ownership
if [ -n "${UID_ARG}" ] || [ -n "${GID_ARG}" ]; then
  chown_args=""
  if [ -n "${UID_ARG}" ] && [ -n "${GID_ARG}" ]; then
    chown_args="${UID_ARG}:${GID_ARG}"
  elif [ -n "${UID_ARG}" ]; then
    chown_args="${UID_ARG}:$(id -g)"
  else
    chown_args="$(id -u):${GID_ARG}"
  fi
  echo "Setting ownership of $DATA_DIR to $chown_args"
  sudo chown -R "$chown_args" "$DATA_DIR"
else
  # If running under sudo, prefer SUDO_UID; else current user
  if [ -n "${SUDO_UID:-}" ]; then
    echo "Detected sudo; setting owner to $SUDO_UID:$SUDO_GID"
    sudo chown -R "$SUDO_UID:$SUDO_GID" "$DATA_DIR"
  else
    echo "Setting owner to current user $(id -u):$(id -g)"
    chown -R "$(id -u):$(id -g)" "$DATA_DIR"
  fi
fi

# Tighten permissions
chmod -R 750 "$DATA_DIR"

echo "Data directory prepared: $DATA_DIR"

# Optionally run docker compose
if [ "$DO_DEPLOY" -eq 1 ]; then
  if command -v docker >/dev/null 2>&1; then
    echo "Running: $COMPOSE_CMD up -d --build"
    $COMPOSE_CMD up -d --build
    echo "Compose started. Use 'docker ps' and 'docker logs -f dealscout-listener' to inspect"
  else
    echo "Docker not found in PATH; cannot run compose" >&2
    exit 1
  fi
else
  echo "To deploy the container now, re-run with --deploy or use Portainer to deploy the stack."
fi

echo "Done."
