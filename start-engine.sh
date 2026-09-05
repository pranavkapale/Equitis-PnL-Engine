#!/usr/bin/env bash
# Equitis PnL Engine - Production Launch Script (Phase 5)

echo "Starting Equitis PnL Engine..."

# Explicit JVM arguments for Java 25 Generational ZGC 
# and memory pre-touching for consistent low latency.
JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xmx4G -Xms4G -XX:+AlwaysPreTouch"

java $JAVA_OPTS -jar target/equities-pnl-engine-1.0-SNAPSHOT.jar
