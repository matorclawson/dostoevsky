#!/bin/sh
set -eu
: "${APP_DIR:?set APP_DIR to the Go module embedding Dostoevsky}"
: "${APP_NAME:?set APP_NAME to the executable filename}"
case "$APP_NAME" in .*|-*|*[!a-zA-Z0-9._-]*) echo "invalid executable name" >&2; exit 2;; esac
case "$ARCH" in amd64|arm64) ;; *) echo "ARCH must be amd64 or arm64" >&2; exit 2;; esac
case "$APP_PACKAGE" in .|./*) ;; *) echo "APP_PACKAGE must be . or a ./package path" >&2; exit 2;; esac
if [ "$1" = install ]; then : "${DESTDIR:?set DESTDIR to the binary installation directory}"; fi
root=$(pwd -P)
app=$(cd "$APP_DIR" && pwd -P)
out="$root/build/deploy/linux-$ARCH"
mkdir -p "$out"
stage=$(mktemp -d "$out/.build.XXXXXX")
install_stage=''
trap 'rm -rf -- "$stage"; if [ -n "$install_stage" ]; then rm -rf -- "$install_stage"; fi' EXIT
export GOOS=linux GOARCH="$ARCH" CGO_ENABLED=0
(
 cd "$app"
 pkg=$("$GO" list -f '{{.Name}}' "$APP_PACKAGE")
 [ "$pkg" = main ] || { echo "APP_PACKAGE must be a main package" >&2; exit 2; }
 "$GO" list -deps "$APP_PACKAGE" > "$stage/packages"
 grep -Fxq github.com/zeroemployeeorg/dostoevsky "$stage/packages" || { echo "host must import Dostoevsky" >&2; exit 2; }
 "$GO" build -trimpath -o "$stage/$APP_NAME" "$APP_PACKAGE"
)
if [ "$1" = install ]; then
 mkdir -p -- "$DESTDIR"
 install_stage=$(mktemp -d "$DESTDIR/.dost-install.XXXXXX")
 install -m 0755 "$stage/$APP_NAME" "$install_stage/$APP_NAME"
 mv -fT -- "$install_stage/$APP_NAME" "$DESTDIR/$APP_NAME"
 echo "Installed $DESTDIR/$APP_NAME"
fi
mv -fT -- "$stage/$APP_NAME" "$out/$APP_NAME"
echo "Built $out/$APP_NAME"
