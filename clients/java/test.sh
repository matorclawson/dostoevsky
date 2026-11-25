#!/bin/sh
# Compiles the client and runs its tests with only a JDK (17 or newer): no Maven, no downloads.
set -eu
cd "$(dirname "$0")"
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
javac --release 17 -d "$out" src/main/java/io/github/rodriveracom/dostoevsky/Client.java src/test/java/io/github/rodriveracom/dostoevsky/ClientTest.java
java -ea -cp "$out" io.github.rodriveracom.dostoevsky.ClientTest
