# Sourced by local tooling and tests: puts the build's values in the environment.
# No value lives in this repository (PLAN §4, §8, §9).
#
# Precedence, first match wins:
#   1. what the calling session already exports
#   2. ~/.config/cdc_flight_v2/env.local   (overrides for this machine)
#   3. ~/.config/cdc_flight_v2/env         (override the path with CDC_V2_ENV_FILE)
# The MotherDuck token is read under its lowercase name; both spellings end up equal.
# A Postgres client that names no port reaches this build's primary, never the service on the default port.

_cdc_v2_load() {
  [ -r "$1" ] || return 0
  while IFS= read -r _cdc_v2_line || [ -n "$_cdc_v2_line" ]; do
    case "$_cdc_v2_line" in '' | \#*) continue ;; esac
    _cdc_v2_line=${_cdc_v2_line#export }
    _cdc_v2_name=${_cdc_v2_line%%=*}
    case "$_cdc_v2_name" in '' | *[!A-Za-z0-9_]*) continue ;; esac
    eval "[ -n \"\${${_cdc_v2_name}+x}\" ]" && continue
    _cdc_v2_value=${_cdc_v2_line#*=}
    case "$_cdc_v2_value" in
      \"*\") _cdc_v2_value=${_cdc_v2_value#\"}; _cdc_v2_value=${_cdc_v2_value%\"} ;;
      \'*\') _cdc_v2_value=${_cdc_v2_value#\'}; _cdc_v2_value=${_cdc_v2_value%\'} ;;
    esac
    export "${_cdc_v2_name}=${_cdc_v2_value}"
  done < "$1"
}

_cdc_v2_cfg="${CDC_V2_CONFIG_DIR:-$HOME/.config/cdc_flight_v2}"
_cdc_v2_load "$_cdc_v2_cfg/env.local"
_cdc_v2_load "${CDC_V2_ENV_FILE:-$_cdc_v2_cfg/env}"

if [ -n "${motherduck_token:-}" ]; then
  export MOTHERDUCK_TOKEN="$motherduck_token"
elif [ -n "${MOTHERDUCK_TOKEN:-}" ]; then
  export motherduck_token="$MOTHERDUCK_TOKEN"
fi

export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-55432}"
export CDC_V2_PG_BIN="${CDC_V2_PG_BIN:-/opt/homebrew/opt/postgresql@18/bin}"
export CDC_V2_PG_HOME="${CDC_V2_PG_HOME:-$HOME/.cache/cdc_flight_v2/pg}"

unset _cdc_v2_cfg _cdc_v2_line _cdc_v2_name _cdc_v2_value
unset -f _cdc_v2_load
