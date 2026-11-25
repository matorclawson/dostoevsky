#!/bin/sh
# Fetches the pinned jars into $1 (default .dev/bench-java/lib). Each is downloaded by exact URL from
# repo1.maven.org and kept only if its SHA-256 equals the one in deps.lock; any difference stops the run.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
lib=${1:-.dev/bench-java/lib}
mkdir -p "$lib"
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
grep -v '^#' "$here/deps.lock" | while read -r gav want; do
  [ -n "$gav" ] || continue
  g=${gav%%:*}; rest=${gav#*:}; a=${rest%%:*}; v=${rest#*:}
  file="$lib/$a-$v.jar"
  if [ -f "$file" ] && [ "$(sha "$file")" = "$want" ]; then continue; fi
  url="https://repo1.maven.org/maven2/$(echo "$g" | tr . /)/$a/$v/$a-$v.jar"
  curl -fsSL --retry 2 -o "$file.part" "$url"
  got=$(sha "$file.part")
  if [ "$got" != "$want" ]; then
    rm -f "$file.part"
    echo "REFUSED $a-$v: sha256 $got, expected $want" >&2
    exit 1
  fi
  mv "$file.part" "$file"
done
echo "jars verified in $lib"
