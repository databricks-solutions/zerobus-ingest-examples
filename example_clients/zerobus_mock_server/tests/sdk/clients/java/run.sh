#!/bin/sh
# Arrow Java needs java.nio opened to its memory module (see arrow.apache.org/docs/java/install.html).
exec java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp "/client/target/classes:/client/target/lib/*" SdkMatrixClient "$@"
