#!/bin/sh
# Compiles the client and runs its tests with only a JDK (17 or newer): no Maven, no downloads.
set -eu
cd "$(dirname "$0")"
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
javac --release 17 -encoding UTF-8 -d "$out" src/main/java/io/github/rodriveracom/dostoevsky/Client.java src/main/java/io/github/rodriveracom/dostoevsky/NativeClient.java \
  src/test/java/io/github/rodriveracom/dostoevsky/ClientTest.java src/test/java/io/github/rodriveracom/dostoevsky/NativeClientTest.java src/test/java/io/github/rodriveracom/dostoevsky/NativeInteropTest.java
java -ea -cp "$out" io.github.rodriveracom.dostoevsky.ClientTest
java -ea -cp "$out" io.github.rodriveracom.dostoevsky.NativeClientTest
java -ea -cp "$out" io.github.rodriveracom.dostoevsky.NativeInteropTest
