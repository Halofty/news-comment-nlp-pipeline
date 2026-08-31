#!/usr/bin/env bash

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
java_home="$project_root/.tools/java17-root/usr/lib/jvm/java-17-openjdk-amd64"

if [[ ! -x "$java_home/bin/java" ]]; then
  echo "Local Java 17 is not installed at $java_home" >&2
  return 1 2>/dev/null || exit 1
fi

export JAVA_HOME="$java_home"
export PATH="$JAVA_HOME/bin:$PATH"
java -version
