#!/bin/sh
set -eu
exec "${ID_NATIVE_BIN:?set ID_NATIVE_BIN to the built native server}" -segmented "$@"
