#!/usr/bin/env bash
set -euo pipefail

# --- Helpers ---------------------------------------------------------------

err() { printf "\e[31mERROR:\e[0m %s\n" "$*" >&2; }
info() { printf "\e[36m==>\e[0m %s\n" "$*"; }

need_poetry_version="1.8"            # Require Poetry 1.8.x
want_python_minor="3.11"             # Target Python version
py311_bin="python3.11"               # Name from altinstall (typically /usr/local/bin/python3.11)

# --- 1) Check Poetry is installed and is version 1.8.x ---------------------

if ! command -v poetry >/dev/null 2>&1; then
  err "Poetry not found. Please install Poetry ${need_poetry_version}.x and re-run."
  exit 1
fi

poetry_ver_raw="$(poetry --version 2>/dev/null || true)"           # e.g., "Poetry (version 1.8.3)"
poetry_ver="$(printf "%s" "$poetry_ver_raw" | sed -n 's/.*version \([0-9.]*\).*/\1/p')"

if ! [[ "$poetry_ver" =~ ^1\.8(\.|$) ]]; then
  err "Poetry ${need_poetry_version}.x required, found '${poetry_ver_raw}'."
  exit 1
fi
info "Poetry ${poetry_ver} OK"

# --- 2) Ensure we're in a Poetry project root ------------------------------

if [[ ! -f "pyproject.toml" ]]; then
  err "pyproject.toml not found in current directory. Run this script from your project root."
  exit 1
fi

# --- 3) Check current Poetry env Python version (if an env already exists) --

env_path="$(poetry env info --path 2>/dev/null || true)"
current_py_ver=""

if [[ -n "$env_path" && -x "$env_path/bin/python" ]]; then
  current_py_ver="$("$env_path/bin/python" -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' 2>/dev/null || true)"
  info "Existing Poetry env detected at: $env_path (Python $current_py_ver)"
else
  info "No existing Poetry virtualenv detected for this project."
fi

# --- 4) If env Python is not 3.11, try to switch to python3.11 --------------

if [[ "$current_py_ver" != "$want_python_minor" ]]; then
  info "Ensuring Poetry uses Python ${want_python_minor}…"
  if command -v "$py311_bin" >/dev/null 2>&1; then
    info "Found ${py311_bin}: $(command -v "$py311_bin")"
    poetry env use "$py311_bin"
  else
    err "Python ${want_python_minor} not found on system. Please install it (e.g., via 'make altinstall') and re-run."
    exit 1
  fi
else
  info "Poetry env already on Python ${want_python_minor}."
fi

# --- 5) Install project dependencies (no root install) ----------------------

info "Installing dependencies with 'poetry install --no-root'…"
poetry install --no-root

info "Done."