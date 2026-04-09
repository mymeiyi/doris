#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# manage_component.sh - Start/stop/restart/status for individual Doris
# components inside an all-in-one Docker container.
#
# Usage: manage_component.sh {start|stop|restart|status} {fdb|ms|recycler|fe|be} [id]

set -e

FDB_HOME="/opt/apache-doris/fdb"
MS_HOME="/opt/apache-doris/ms"
FE_HOME="/opt/apache-doris/fe"
BE_HOME="/opt/apache-doris/be"
STATUS_DIR="${STATUS_DIR:-/opt/apache-doris/status}"
INTENTIONAL_STOP_DIR="$STATUS_DIR/intentional_stops"

ACTION="$1"
COMPONENT="$2"
COMPONENT_ID="${3:-1}"

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') [manage_component] $*"
}

usage() {
    echo "Usage: $0 {start|stop|restart|status} {fdb|ms|recycler|fe|be} [id]"
    exit 1
}

# ---------------------------------------------------------------------------
# Process detection helpers
# ---------------------------------------------------------------------------

pid_of_fdb() {
    pgrep -f fdbmonitor 2>/dev/null || true
}

pid_of_ms() {
    # MS runs _cloud with --meta-service; exclude recycler processes
    pgrep -af '_cloud' 2>/dev/null | grep -v recycler | grep 'meta.service' | awk '{print $1}' || true
}

pid_of_recycler() {
    pgrep -af '_cloud' 2>/dev/null | grep 'recycler' | awk '{print $1}' || true
}

pid_of_fe() {
    pgrep -f 'org.apache.doris.DorisFE' 2>/dev/null || true
}

pid_of_be() {
    pgrep doris_be 2>/dev/null || true
}

is_running() {
    local pid
    pid=$("pid_of_$1")
    [ -n "$pid" ]
}

# ---------------------------------------------------------------------------
# Stop helpers
# ---------------------------------------------------------------------------

stop_fdb() {
    local pid
    pid=$(pid_of_fdb)
    if [ -z "$pid" ]; then
        log "fdb is not running"
        return 0
    fi
    log "stopping fdb (pid $pid)"
    kill "$pid" 2>/dev/null || true
    # Wait for process to exit
    for i in $(seq 1 30); do
        if ! kill -0 "$pid" 2>/dev/null; then
            log "fdb stopped"
            return 0
        fi
        sleep 1
    done
    log "fdb did not stop in time, sending SIGKILL"
    kill -9 "$pid" 2>/dev/null || true
    log "fdb killed"
}

stop_ms() {
    local pid
    pid=$(pid_of_ms)
    if [ -z "$pid" ]; then
        log "ms is not running"
        return 0
    fi
    log "stopping ms"
    cd "$MS_HOME" && bash bin/stop.sh 2>/dev/null || true
    # Verify it stopped
    for i in $(seq 1 15); do
        pid=$(pid_of_ms)
        if [ -z "$pid" ]; then
            log "ms stopped"
            return 0
        fi
        sleep 1
    done
    # Force kill if still running
    pid=$(pid_of_ms)
    if [ -n "$pid" ]; then
        log "ms did not stop in time, force killing (pid $pid)"
        kill -9 $pid 2>/dev/null || true
    fi
    log "ms stopped"
}

stop_recycler() {
    local pid
    pid=$(pid_of_recycler)
    if [ -z "$pid" ]; then
        log "recycler is not running"
        return 0
    fi
    log "stopping recycler (pid $pid)"
    kill "$pid" 2>/dev/null || true
    for i in $(seq 1 30); do
        if ! kill -0 "$pid" 2>/dev/null; then
            log "recycler stopped"
            return 0
        fi
        sleep 1
    done
    log "recycler did not stop in time, sending SIGKILL"
    kill -9 "$pid" 2>/dev/null || true
    log "recycler killed"
}

stop_fe() {
    local pid
    pid=$(pid_of_fe)
    if [ -z "$pid" ]; then
        log "fe is not running"
        return 0
    fi
    log "stopping fe"
    cd "$FE_HOME"
    if [ "$STOP_GRACE" = "1" ]; then
        bash bin/stop_fe.sh --grace 2>/dev/null || true
    else
        bash bin/stop_fe.sh 2>/dev/null || true
    fi
    for i in $(seq 1 30); do
        pid=$(pid_of_fe)
        if [ -z "$pid" ]; then
            log "fe stopped"
            return 0
        fi
        sleep 1
    done
    pid=$(pid_of_fe)
    if [ -n "$pid" ]; then
        log "fe did not stop in time, force killing (pid $pid)"
        kill -9 $pid 2>/dev/null || true
    fi
    log "fe stopped"
}

stop_be() {
    local pid
    pid=$(pid_of_be)
    if [ -z "$pid" ]; then
        log "be is not running"
        return 0
    fi
    log "stopping be"
    cd "$BE_HOME"
    if [ "$STOP_GRACE" = "1" ]; then
        bash bin/stop_be.sh --grace 2>/dev/null || true
    else
        bash bin/stop_be.sh 2>/dev/null || true
    fi
    for i in $(seq 1 30); do
        pid=$(pid_of_be)
        if [ -z "$pid" ]; then
            log "be stopped"
            return 0
        fi
        sleep 1
    done
    pid=$(pid_of_be)
    if [ -n "$pid" ]; then
        log "be did not stop in time, force killing (pid $pid)"
        kill -9 $pid 2>/dev/null || true
    fi
    log "be stopped"
}

# ---------------------------------------------------------------------------
# Start helpers
# ---------------------------------------------------------------------------

start_fdb() {
    if is_running fdb; then
        log "fdb is already running (pid $(pid_of_fdb))"
        return 0
    fi
    log "starting fdb"
    fdbmonitor --conffile "${FDB_HOME}/conf/fdb.conf" \
               --lockfile "${FDB_HOME}/fdbmonitor.pid" &
    sleep 1
    if is_running fdb; then
        log "fdb started (pid $(pid_of_fdb))"
    else
        log "fdb failed to start"
        return 1
    fi
}

start_ms() {
    if is_running ms; then
        log "ms is already running (pid $(pid_of_ms))"
        return 0
    fi
    log "starting ms"
    cd "$MS_HOME" && bash bin/start.sh --meta-service --daemon
    sleep 1
    if is_running ms; then
        log "ms started (pid $(pid_of_ms))"
    else
        log "ms failed to start"
        return 1
    fi
}

start_recycler() {
    if is_running recycler; then
        log "recycler is already running (pid $(pid_of_recycler))"
        return 0
    fi
    log "starting recycler"
    cd "$MS_HOME" && bash bin/start.sh --recycler --daemon
    sleep 1
    if is_running recycler; then
        log "recycler started (pid $(pid_of_recycler))"
    else
        log "recycler failed to start"
        return 1
    fi
}

start_fe() {
    if is_running fe; then
        log "fe is already running (pid $(pid_of_fe))"
        return 0
    fi
    log "starting fe"
    cd "$FE_HOME" && bash bin/start_fe.sh --daemon
    sleep 1
    if is_running fe; then
        log "fe started (pid $(pid_of_fe))"
    else
        log "fe failed to start"
        return 1
    fi
}

start_be() {
    if is_running be; then
        log "be is already running (pid $(pid_of_be))"
        return 0
    fi
    log "starting be"
    cd "$BE_HOME" && bash bin/start_be.sh --daemon
    sleep 1
    if is_running be; then
        log "be started (pid $(pid_of_be))"
    else
        log "be failed to start"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

status_component() {
    local comp="$1"
    if is_running "$comp"; then
        echo "running"
    else
        echo "stopped"
    fi
}

# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------

if [ -z "$ACTION" ] || [ -z "$COMPONENT" ]; then
    usage
fi

case "$ACTION" in
    start)
        # Remove intentional stop marker
        rm -f "$INTENTIONAL_STOP_DIR/$COMPONENT"
        case "$COMPONENT" in
            fdb)      start_fdb ;;
            ms)       start_ms ;;
            recycler) start_recycler ;;
            fe)       start_fe ;;
            be)       start_be ;;
            *)        usage ;;
        esac
        ;;
    stop)
        # Create intentional stop marker so monitor loop doesn't kill container
        mkdir -p "$INTENTIONAL_STOP_DIR"
        touch "$INTENTIONAL_STOP_DIR/$COMPONENT"
        case "$COMPONENT" in
            fdb)      stop_fdb ;;
            ms)       stop_ms ;;
            recycler) stop_recycler ;;
            fe)       stop_fe ;;
            be)       stop_be ;;
            *)        usage ;;
        esac
        ;;
    restart)
        case "$COMPONENT" in
            fdb)      stop_fdb && start_fdb ;;
            ms)       stop_ms && start_ms ;;
            recycler) stop_recycler && start_recycler ;;
            fe)       stop_fe && start_fe ;;
            be)       stop_be && start_be ;;
            *)        usage ;;
        esac
        # Remove intentional stop marker after restart
        rm -f "$INTENTIONAL_STOP_DIR/$COMPONENT"
        ;;
    status)
        case "$COMPONENT" in
            fdb|ms|recycler|fe|be) status_component "$COMPONENT" ;;
            *)                     usage ;;
        esac
        ;;
    *)
        usage
        ;;
esac

exit 0
