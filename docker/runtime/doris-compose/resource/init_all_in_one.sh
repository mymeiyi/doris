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

# All-in-one init script: starts FDB, MS, Recycler, FE, and BE(s) in
# a single container.  Invoked by entrypoint.sh as:
#   bash /opt/apache-doris/resource/entrypoint.sh init_all_in_one.sh

DIR=$(
    cd $(dirname $0)
    pwd
)

# common.sh reads MY_TYPE, MY_ID, and DORIS_HOME to derive status paths.
export MY_TYPE="all_in_one"
export MY_ID="1"
export DORIS_HOME="${FE_HOME}"

source "$DIR/common.sh"

# Override status paths to use the shared status directory so that every
# component (FDB, MS, FE, BE) writes to and reads from the same location.
export STATUS_DIR="${STATUS_DIR:-/opt/apache-doris/status}"
export MASTER_FE_QUERY_ADDR_FILE="$STATUS_DIR/master_fe_query_addr"
export HAS_INIT_FDB_FILE="$STATUS_DIR/has_init_fdb"
export HAS_CREATE_INSTANCE_FILE="$STATUS_DIR/has_create_instance"
export LOG_FILE="$STATUS_DIR/health.out"
export LOCK_FILE="$STATUS_DIR/token"
export MY_TYPE_ID="${MY_TYPE}-${MY_ID}"

FDB_PORT="${FDB_PORT:-4500}"
MS_PORT="${MS_PORT:-5000}"
META_SERVICE_ENDPOINT="${META_SERVICE_ENDPOINT:-${MY_IP}:${MS_PORT}}"

FDBMONITOR_PID=""
FE_DAEMON_PID=""

# =====================================================================
# Status directory setup
# =====================================================================

setup_status_dirs() {
    mkdir -p "$STATUS_DIR"
    # Only create symlinks for paths where the parent dir is writable.
    # FE and BE home dirs have mounted volumes so they are writable.
    # FDB and recycler parent dirs are owned by root in the image,
    # but that's OK since init_all_in_one.sh overrides all status paths via env vars.
    for home in "$FE_HOME" "$BE_HOME" "$MS_HOME"; do
        mkdir -p "$home/log" 2>/dev/null
        if [ -d "$home/status" ] && [ ! -L "$home/status" ]; then
            rm -rf "$home/status"
        fi
        ln -sfn "$STATUS_DIR" "$home/status" 2>/dev/null || true
    done
    health_log "status directory symlinks created"
}

# =====================================================================
# FDB
# =====================================================================

start_fdb() {
    health_log "=== Starting FDB ==="

    mkdir -p "$FDB_HOME/log" "$FDB_HOME/data"

    # Fix fdb.conf: replace /usr/bin paths with FDB_HOME/bin for all-in-one mode
    sed -i "s|command = /usr/bin/fdbserver|command = $FDB_HOME/bin/fdbserver|g" "$FDB_HOME/conf/fdb.conf"
    sed -i "s|command = /usr/bin/backup_agent|command = $FDB_HOME/bin/backup_agent|g" "$FDB_HOME/conf/fdb.conf"
    # Fix user/group to match current user (gosu switches away from root)
    sed -i "s|user = root|user = $(whoami)|g" "$FDB_HOME/conf/fdb.conf"
    sed -i "s|group = root|group = $(id -gn)|g" "$FDB_HOME/conf/fdb.conf"

    "$FDB_HOME/bin/fdbmonitor" \
        --conffile "$FDB_HOME/conf/fdb.conf" \
        --lockfile "$FDB_HOME/data/fdbmonitor.pid" &
    FDBMONITOR_PID=$!
    health_log "fdbmonitor started with PID $FDBMONITOR_PID"

    if [ -f "$HAS_INIT_FDB_FILE" ]; then
        health_log "FDB already initialized, skip configure"
        return
    fi

    for ((i = 0; i < 30; i++)); do
        "$FDB_HOME/bin/fdbcli" \
            -C "$FDB_HOME/conf/fdb.cluster" \
            --exec 'configure new single ssd' 2>&1
        if [ $? -eq 0 ]; then
            touch "$HAS_INIT_FDB_FILE"
            health_log "fdbcli init cluster succ"
            return
        fi
        health_log "fdbcli init cluster attempt $((i + 1)) failed, retrying..."
        sleep 1
    done

    health_log "fdbcli init cluster failed after 30 attempts, exit"
    exit 1
}

# =====================================================================
# Meta Service
# =====================================================================

wait_ms_ready() {
    health_log "Waiting for Meta Service to be ready..."
    for ((i = 0; i < 60; i++)); do
        curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/get_instance?token=greedisgood9999&instance_id=check_ready" \
            >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            health_log "Meta Service is responding"
            return
        fi
        sleep 1
    done
    health_log "Meta Service not ready after 60 seconds, exit"
    exit 1
}

start_ms() {
    health_log "=== Starting Meta Service ==="

    export DORIS_HOME="$MS_HOME"
    cd "$MS_HOME"

    bash bin/start.sh --meta-service --daemon 2>&1 | tee -a "$MS_HOME/log/doris_cloud.out"

    wait_ms_ready

    if [ -f "$HAS_CREATE_INSTANCE_FILE" ]; then
        health_log "Instance already created"
    elif [ "${SQL_MODE_NODE_MGR}" = "1" ]; then
        health_log "SQL_MODE_NODE_MGR is set, skipping create_instance"
        touch "$HAS_CREATE_INSTANCE_FILE"
    else
        create_doris_instance
    fi
}

# =====================================================================
# Recycler
# =====================================================================

start_recycler() {
    health_log "=== Starting Recycler ==="

    # Recycler shares the same binary as MS but needs its own conf/log.
    # Create /opt/apache-doris/recycler as a DORIS_HOME with symlinked bin/lib.
    local recycler_home="/opt/apache-doris/recycler"
    mkdir -p "$recycler_home/log"
    ln -sfn "$MS_HOME/bin" "$recycler_home/bin"
    ln -sfn "$MS_HOME/lib" "$recycler_home/lib"
    ln -sfn "$MS_HOME/conf" "$recycler_home/conf"
    if [ ! -L "$recycler_home/status" ]; then
        ln -sfn "$STATUS_DIR" "$recycler_home/status"
    fi

    export DORIS_HOME="$recycler_home"
    cd "$recycler_home"

    bash bin/start.sh --recycler --daemon 2>&1 | tee -a "$recycler_home/log/doris_cloud.out"

    health_log "Recycler started"
}

# =====================================================================
# FE helpers
# =====================================================================

register_fe_with_ms() {
    local cloud_unique_id="$1"

    local nodes='{
        "cloud_unique_id": "'"${cloud_unique_id}"'",
        "ip": "'"${MY_IP}"'",
        "edit_log_port": "'"${MY_EDITLOG_PORT}"'",
        "node_type": "FE_MASTER"
    }'

    lock_cluster

    local output
    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/add_cluster?token=greedisgood9999" \
        -d '{"instance_id": "'"${INSTANCE_ID}"'",
        "cluster": {
            "type": "SQL",
            "cluster_name": "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
            "cluster_id": "RESERVED_CLUSTER_ID_FOR_SQL_SERVER",
            "nodes": ['"${nodes}"']
        }}')

    unlock_cluster

    health_log "FE add_cluster output: $output"
    local code
    code=$(jq -r '.code' <<<"$output")

    if [ "$code" != "OK" ] && [ "$code" != "ALREADY_EXISTED" ]; then
        health_log "FE register with MS failed, exit"
        exit 1
    fi

    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/get_cluster?token=greedisgood9999" \
        -d '{"instance_id": "'"${INSTANCE_ID}"'",
            "cloud_unique_id": "'"${cloud_unique_id}"'",
            "cluster_name": "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
            "cluster_id": "RESERVED_CLUSTER_ID_FOR_SQL_SERVER"}')

    health_log "FE get_cluster output: $output"
    code=$(jq -r '.code' <<<"$output")

    if [ "$code" != "OK" ]; then
        health_log "FE get cluster verification failed, exit"
        exit 1
    fi
}

# Monitors FE master status and writes master address file, identical to
# the fe_daemon in init_fe.sh.
fe_daemon() {
    set +e
    while true; do
        sleep 1
        local output
        output=$(mysql -P "$MY_QUERY_PORT" -h "$MY_IP" -u root --execute "SHOW FRONTENDS;" 2>&1)
        if [ $? -ne 0 ]; then
            continue
        fi
        local header
        header=$(grep IsMaster <<<"$output")
        if [ $? -ne 0 ]; then
            continue
        fi
        local host_index=-1
        local is_master_index=-1
        local query_port_index=-1
        local i=1
        local field
        for field in $header; do
            [[ "$field" = "Host" ]] && host_index=$i
            [[ "$field" = "IsMaster" ]] && is_master_index=$i
            [[ "$field" = "QueryPort" ]] && query_port_index=$i
            ((i = i + 1))
        done
        if [ $host_index -eq -1 ] || [ $is_master_index -eq -1 ] || [ $query_port_index -eq -1 ]; then
            continue
        fi
        echo "$output" | awk -v query_port="$query_port_index" \
            -v is_master="$is_master_index" \
            -v host="$host_index" \
            '{print $query_port $is_master $host}' |
            grep "$MY_QUERY_PORT" | grep "$MY_IP" | grep true >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "${MY_IP}:${MY_QUERY_PORT}" >"$MASTER_FE_QUERY_ADDR_FILE"
            if [ "$MASTER_FE_IP" != "$MY_IP" ] || [ "$MASTER_FE_PORT" != "$MY_QUERY_PORT" ]; then
                health_log "FE is master at ${MY_IP}:${MY_QUERY_PORT}"
                MASTER_FE_IP="$MY_IP"
                MASTER_FE_PORT="$MY_QUERY_PORT"
            fi
        fi
    done
}

wait_fe_ready() {
    health_log "Waiting for FE to be ready..."
    for ((i = 0; i < 120; i++)); do
        if [ -f "$MASTER_FE_QUERY_ADDR_FILE" ]; then
            local addr
            addr=$(cat "$MASTER_FE_QUERY_ADDR_FILE")
            if [ -n "$addr" ]; then
                MASTER_FE_IP=$(echo "$addr" | cut -d ":" -f 1)
                MASTER_FE_PORT=$(echo "$addr" | cut -d ":" -f 2)
                health_log "FE is ready at $addr"
                return
            fi
        fi
        sleep 1
    done
    health_log "FE not ready after 120 seconds, exit"
    exit 1
}

# =====================================================================
# FE startup
# =====================================================================

start_fe() {
    health_log "=== Starting FE ==="

    local fe_register_file="$STATUS_DIR/fe-1-register"
    local cloud_unique_id="${CLOUD_UNIQUE_ID_FE_1}"

    export DORIS_HOME="$FE_HOME"
    export DORIS_TDE_AK="${TDE_AK}"
    export DORIS_TDE_SK="${TDE_SK}"
    cd "$FE_HOME"

    if [ ! -f "$fe_register_file" ]; then
        if [ "${SQL_MODE_NODE_MGR}" != "1" ]; then
            register_fe_with_ms "$cloud_unique_id"
        fi
        touch "$fe_register_file"
    fi

    # Clear stale readiness file so wait_fe_ready doesn't see the old run.
    rm -f "$MASTER_FE_QUERY_ADDR_FILE"

    fe_daemon &
    FE_DAEMON_PID=$!

    health_log "Running start_fe.sh"
    bash "$FE_HOME/bin/start_fe.sh" --daemon 2>&1 | tee -a "$FE_HOME/log/fe.out"

    wait_fe_ready
}

# =====================================================================
# BE helpers
# =====================================================================

register_be_with_ms() {
    local be_id="$1"
    local cloud_unique_id="$2"
    local heartbeat_port="$3"
    local cluster_name="${4:-${CLUSTER_NAME:-compute_cluster}}"
    local cluster_id="${cluster_name}_id"

    local nodes='{
        "cloud_unique_id": "'"${cloud_unique_id}"'",
        "ip": "'"${MY_IP}"'",
        "heartbeat_port": "'"${heartbeat_port}"'"
    }'

    lock_cluster

    local output
    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/add_cluster?token=greedisgood9999" \
        -d '{"instance_id": "'"${INSTANCE_ID}"'",
        "cluster": {
            "type": "COMPUTE",
            "cluster_name": "'"${cluster_name}"'",
            "cluster_id": "'"${cluster_id}"'",
            "nodes": ['"${nodes}"']
        }}')

    health_log "BE-${be_id} add_cluster output: $output"
    local code
    code=$(jq -r '.code' <<<"$output")

    if [ "$code" == "ALREADY_EXISTED" ]; then
        output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/add_node?token=greedisgood9999" \
            -d '{"instance_id": "'"${INSTANCE_ID}"'",
            "cluster": {
                "type": "COMPUTE",
                "cluster_name": "'"${cluster_name}"'",
                "cluster_id": "'"${cluster_id}"'",
                "nodes": ['"${nodes}"']
            }}')
        health_log "BE-${be_id} add_node output: $output"
        code=$(jq -r '.code' <<<"$output")
    fi

    unlock_cluster

    if [ "$code" != "OK" ] && [ "$code" != "ALREADY_EXISTED" ]; then
        health_log "BE-${be_id} register with MS failed, exit"
        exit 1
    fi

    output=$(curl -s "${META_SERVICE_ENDPOINT}/MetaService/http/get_cluster?token=greedisgood9999" \
        -d '{"instance_id": "'"${INSTANCE_ID}"'",
            "cloud_unique_id": "'"${cloud_unique_id}"'",
            "cluster_name": "'"${cluster_name}"'",
            "cluster_id": "'"${cluster_id}"'"}')

    health_log "BE-${be_id} get_cluster output: $output"
    code=$(jq -r '.code' <<<"$output")

    if [ "$code" != "OK" ]; then
        health_log "BE-${be_id} get cluster verification failed, exit"
        exit 1
    fi
}

register_be_with_fe() {
    local be_id="$1"
    local heartbeat_port="$2"

    while true; do
        local output
        output=$(mysql -P "$MASTER_FE_PORT" -h "$MASTER_FE_IP" -u root \
            --execute "ALTER SYSTEM ADD BACKEND '${MY_IP}:${heartbeat_port}';" 2>&1)
        local res=$?
        health_log "BE-${be_id} register with FE: $output"
        [ $res -eq 0 ] && break
        (echo "$output" | grep "Same backend already exists") && break
        sleep 1
    done
}

# =====================================================================
# BE startup
# =====================================================================

start_bes() {
    health_log "=== Starting BEs ==="

    for ((be_id = 1; be_id <= ${BE_NUM:-1}; be_id++)); do
        local be_register_file="$STATUS_DIR/be-${be_id}-register"
        local cloud_uid_var="CLOUD_UNIQUE_ID_BE_${be_id}"
        local cloud_unique_id="${!cloud_uid_var}"
        local heartbeat_port="${MY_HEARTBEAT_PORT}"
        local cluster_name_var="CLUSTER_NAME_BE_${be_id}"
        local cluster_name="${!cluster_name_var:-${CLUSTER_NAME:-compute_cluster}}"

        export DORIS_HOME="$BE_HOME"
        cd "$BE_HOME"

        if [ -n "$LLVM_PROFILE_FILE_PREFIX" ]; then
            export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE_PREFIX}-be${be_id}-$(date +%s)"
        fi

        if [ ! -f "$be_register_file" ]; then
            if [ "${SQL_MODE_NODE_MGR}" != "1" ]; then
                register_be_with_ms "$be_id" "$cloud_unique_id" "$heartbeat_port" "$cluster_name"
                # In cloud mode, FE picks up backends from MS automatically.
                # ALTER SYSTEM ADD BACKEND is only needed for non-cloud mode.
            fi
            touch "$be_register_file"
        fi

        health_log "Starting BE-${be_id}"
        bash "$BE_HOME/bin/start_be.sh" --daemon 2>&1 | tee -a "$BE_HOME/log/be.out"
        health_log "BE-${be_id} start command completed"
    done
}

# =====================================================================
# Graceful shutdown — stop in reverse order
# =====================================================================

stop_all() {
    health_log "=== Graceful shutdown initiated ==="

    # Kill fe_daemon background process first.
    if [ -n "$FE_DAEMON_PID" ]; then
        kill "$FE_DAEMON_PID" 2>/dev/null
        wait "$FE_DAEMON_PID" 2>/dev/null
    fi

    # 1. Stop BEs (reverse order)
    export DORIS_HOME="$BE_HOME"
    for ((be_id = ${BE_NUM:-1}; be_id >= 1; be_id--)); do
        health_log "Stopping BE-${be_id}"
        cd "$BE_HOME" 2>/dev/null
        if [ "$STOP_GRACE" = "1" ]; then
            bash "$BE_HOME/bin/stop_be.sh" --grace 2>&1 || true
        else
            bash "$BE_HOME/bin/stop_be.sh" 2>&1 || true
        fi
    done

    # 2. Stop FE
    export DORIS_HOME="$FE_HOME"
    health_log "Stopping FE"
    cd "$FE_HOME" 2>/dev/null
    if [ "$STOP_GRACE" = "1" ]; then
        bash "$FE_HOME/bin/stop_fe.sh" --grace 2>&1 || true
    else
        bash "$FE_HOME/bin/stop_fe.sh" 2>&1 || true
    fi

    # 3. Stop Recycler and Meta Service
    health_log "Stopping Recycler"
    local recycler_home="/opt/apache-doris/recycler"
    if [ -x "$recycler_home/bin/stop.sh" ] || [ -L "$recycler_home/bin" ]; then
        cd "$recycler_home" 2>/dev/null
        DORIS_HOME="$recycler_home" bash "$recycler_home/bin/stop.sh" 2>&1 || true
    fi

    health_log "Stopping Meta Service"
    export DORIS_HOME="$MS_HOME"
    cd "$MS_HOME" 2>/dev/null
    bash "$MS_HOME/bin/stop.sh" 2>&1 || true

    # 4. Stop FDB
    health_log "Stopping FDB"
    if [ -n "$FDBMONITOR_PID" ]; then
        kill "$FDBMONITOR_PID" 2>/dev/null
        wait "$FDBMONITOR_PID" 2>/dev/null
    fi

    health_log "=== All components stopped ==="
    exit 0
}

# =====================================================================
# Process monitoring loop
# =====================================================================

monitor_processes() {
    health_log "Entering process monitoring loop"
    while true; do
        sleep 30 &
        wait $! 2>/dev/null || true

        # fdbmonitor
        if [ -n "$FDBMONITOR_PID" ] && ! kill -0 "$FDBMONITOR_PID" 2>/dev/null; then
            health_log "ERROR: fdbmonitor (PID $FDBMONITOR_PID) died unexpectedly"
            # break
        fi

        # Meta Service / Recycler (_cloud process)
        if ! pgrep -f "_cloud" >/dev/null 2>&1; then
            health_log "ERROR: Cloud process (MS/Recycler) died unexpectedly"
            # break
        fi

        # FE (java DorisFE)
        if ! ps -elf | grep java | grep "org.apache.doris.DorisFE" | grep -v grep >/dev/null 2>&1; then
            health_log "ERROR: FE process died unexpectedly"
            # break
        fi

        # BE (doris_be)
        if ! pgrep doris_be >/dev/null 2>&1; then
            health_log "ERROR: BE process died unexpectedly"
            # break
        fi
    done

    health_log "Process monitoring detected failure"
    health_log "ps -elf output:\n$(ps -elf)\n"
    health_log "dmesg -T tail:"
    dmesg -T 2>/dev/null | tail -n 50 | tee -a "$LOG_FILE"
}

# =====================================================================
# Main
# =====================================================================

main() {
    trap stop_all SIGTERM

    setup_status_dirs

    start_fdb
    start_ms
    start_recycler
    start_fe
    start_bes

    health_log "=== All components started successfully ==="

    monitor_processes

    health_log "Exiting due to process failure, cleaning up..."
    stop_all
}

main
