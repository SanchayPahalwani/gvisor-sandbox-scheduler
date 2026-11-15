#!/usr/bin/env bash
# Starts and stops a local cluster in the Linux VM: one scheduler and one node agent
# per entry of GSS_NODES, each a transient systemd unit (spec section 11), so they
# outlive this shell and log to the journal (journalctl -u gss-node-a1).
#
#   cluster.sh up               start every unit that is not running, then wait until
#                               every node is READY with its warm pool full (120 s at most);
#                               fails at once if a running unit has other flags
#   cluster.sh down             stop every gss unit, then remove what they left: cgroups
#                               under /sys/fs/cgroup/gss, gss-* namespaces, inet gss_*
#                               nft tables and the runsc roots under /run/gss; a node whose
#                               /run/gss/<id>.lock is held (an agent outside systemd) is
#                               skipped, and down then fails
#   cluster.sh status           the units and the scheduler's view of the nodes
#   cluster.sh kill-group G     SIGKILL every node of group G (in GSS_NODES) and its sandboxes
#   cluster.sh kill-node ID     SIGKILL node ID and its sandboxes (spec D3)
#
# Environment:
#   GSS_NODES       "id:group:port ...", default "a1:a:7101 a2:a:7102 b1:b:7103 b2:b:7104";
#                   the node at index i gets the egress range 10.200.<16*i>.0/20
#   GSS_NODE_CPU    declared CPU per node in millicores (default 4000)
#   GSS_NODE_MEM    declared memory per node in MiB (default 2048)
#   GSS_NODE_MAX    most sandboxes per node (default 16)
#   GSS_POOL        warm sandboxes per node (default 2)
#   GSS_SCHED_ARGS  extra scheduler flags, split on spaces, e.g. "--policy spread" (spec D35);
#                   the scheduler keeps its default --listen 127.0.0.1:7000, where the nodes and
#                   gssctl reach it
#   OTEL_EXPORTER_OTLP_ENDPOINT  passed to every unit when set (spec section 9)
#
# Linux only; run it as the VM user (it uses sudo) after make build. make up, make down
# and make status run it, from macOS inside the VM. Progress goes to stderr; status
# prints to stdout.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
gssctl=$repo/bin/gssctl
sched_addr=127.0.0.1:7000
sched_unit=gss-scheduler.service
up_timeout=120 # seconds
cgroup_dir=/sys/fs/cgroup/gss
run_dir=/run/gss

# Names this project gives: node IDs as cmd/nodeagent accepts them, sandbox namespaces
# gss-<node>-<8 hex> and nft tables gss_<node with - as _> (internal/netpolicy).
node_id_re='^[A-Za-z0-9][A-Za-z0-9-]{0,31}$'
group_re='^[A-Za-z0-9][A-Za-z0-9._-]*$'
port_re='^[1-9][0-9]{0,4}$'
netns_re='^gss-[A-Za-z0-9][A-Za-z0-9-]{0,31}-[0-9a-f]{8}$'
table_re='^gss_[A-Za-z0-9_]{1,32}$'

say() { echo "cluster.sh: $*" >&2; }
die() {
	say "$*"
	exit 1
}

usage() {
	cat >&2 <<'EOF'
usage: scripts/cluster.sh up | down | status | kill-group GROUP | kill-node ID
EOF
	exit 2
}

need_linux() {
	if [[ "$(uname -s)" != Linux ]]; then
		die "run this on Linux (make up, make down and make status do that from macOS)"
	fi
}

# The topology, in GSS_NODES order: node_ids[i], node_groups[i], node_ports[i].
node_ids=()
node_groups=()
node_ports=()

parse_nodes() {
	local spec=${GSS_NODES:-a1:a:7101 a2:a:7102 b1:b:7103 b2:b:7104}
	local entries=() entry id group port extra i
	read -ra entries <<<"$spec"
	if [[ ${#entries[@]} -eq 0 ]]; then
		die "GSS_NODES is empty"
	fi
	if [[ ${#entries[@]} -gt 16 ]]; then
		die "GSS_NODES has ${#entries[@]} nodes; at most 16 fit the egress ranges 10.200.<16*i>.0/20"
	fi
	for entry in "${entries[@]}"; do
		id="" group="" port="" extra=""
		IFS=: read -r id group port extra <<<"$entry"
		if [[ -n $extra || ! $id =~ $node_id_re || ! $group =~ $group_re || ! $port =~ $port_re ]] || ((port > 65535)); then
			die "GSS_NODES entry \"$entry\" is not id:group:port (id: 1 to 32 of [A-Za-z0-9-], starting with a letter or digit)"
		fi
		if [[ $port == "${sched_addr##*:}" ]]; then
			die "GSS_NODES entry \"$entry\": port $port is the scheduler's"
		fi
		for ((i = 0; i < ${#node_ids[@]}; i++)); do
			if [[ ${node_ids[i]} == "$id" || ${node_ports[i]} == "$port" ]]; then
				die "GSS_NODES entry \"$entry\" repeats a node ID or port"
			fi
		done
		node_ids+=("$id")
		node_groups+=("$group")
		node_ports+=("$port")
	done
}

# int_env NAME DEFAULT MIN prints $NAME, or DEFAULT when it is unset or empty, after
# checking that it is an integer of at least MIN.
int_env() {
	local v=${!1:-$2}
	if [[ ! $v =~ ^(0|[1-9][0-9]{0,8})$ ]] || ((v < $3)); then
		die "$1 must be an integer of at least $3, got \"$v\""
	fi
	echo "$v"
}

# unit_lines prints "unit active-state sub-state" for every gss unit systemd has loaded.
unit_lines() {
	systemctl list-units --all --plain --no-legend --type=service "$sched_unit" 'gss-node-*.service' |
		awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^gss-(scheduler|node-.+)\.service$/) { print $i, $(i + 2), $(i + 3); break } }'
}

unit_running() {
	local state
	state=$(systemctl show --property=ActiveState --value "$1" 2>/dev/null || true)
	[[ $state == active || $state == activating || $state == reloading ]]
}

# unit_spec I sets unit, unit_opts (systemd-run options) and unit_cmd for the
# scheduler (I = -1) or node I of GSS_NODES. It reads up's settings.
unit_spec() {
	if (($1 < 0)); then
		unit=$sched_unit
		# The scheduler needs no root; it runs as the user who started the cluster.
		unit_opts=(--uid="$(id -u)" --gid="$(id -g)" ${env_args[@]+"${env_args[@]}"})
		unit_cmd=("$repo/bin/scheduler" ${sched_args[@]+"${sched_args[@]}"})
		return
	fi
	local i=$1
	unit=gss-node-${node_ids[i]}.service
	# Node agents run as root (runsc needs it). KillMode=mixed: a stop sends SIGTERM to
	# the agent alone, which destroys its sandboxes and cleans up; whatever is left in
	# the unit when it exits gets SIGKILL.
	unit_opts=(--property=KillMode=mixed ${env_args[@]+"${env_args[@]}"})
	unit_cmd=("$repo/bin/nodeagent" --node-id "${node_ids[i]}" --group "${node_groups[i]}"
		--listen "127.0.0.1:${node_ports[i]}" --scheduler "$sched_addr"
		--cpu-millis "$cpu" --memory-mib "$mem" --max-sandboxes "$max" --warm-pool-size "$pool"
		--egress-cidr "10.200.$((16 * i)).0/20")
}

# exec_argv UNIT prints the command line a unit runs, from systemd's ExecStart
# "{ path=... ; argv[]=<the arguments, joined by spaces> ; ignore_errors=no ; ... }",
# or nothing when it cannot be read. No argument here holds a space.
exec_argv() {
	systemctl show --property=ExecStart --value "$1" 2>/dev/null |
		sed -n 's/.*argv\[\]=\(.*\) ; ignore_errors=.*/\1/p' | head -n 1
}

# check_unit fails when $unit runs, but with another command line than unit_cmd: a
# cluster of another shape is up, and adding to it would mix the two.
check_unit() {
	unit_running "$unit" || return 0
	local running
	running=$(exec_argv "$unit")
	if [[ -z $running ]]; then
		say "$unit is running already; its command line could not be read to compare, so it is left as it is"
	elif [[ $running != "${unit_cmd[*]}" ]]; then
		say "$unit runs: $running"
		say "this up would run: ${unit_cmd[*]}"
		die "a cluster with a different topology is running; run scripts/cluster.sh down first"
	fi
}

# start_unit starts $unit as a transient unit running unit_cmd, unless it is running
# already: up leaves running units alone.
start_unit() {
	local state
	state=$(systemctl show --property=ActiveState --value "$unit" 2>/dev/null || true)
	case $state in
	active | activating | reloading)
		say "$unit is running already; left as it is"
		return 0
		;;
	deactivating)
		die "$unit is stopping; wait for it, or run scripts/cluster.sh down"
		;;
	esac
	sudo systemctl reset-failed "$unit" >/dev/null 2>&1 || true
	sudo systemd-run --quiet --collect --unit="$unit" ${unit_opts[@]+"${unit_opts[@]}"} "${unit_cmd[@]}"
	say "started $unit"
}

# not_ready TABLE POOL prints, for each node of GSS_NODES that the `gssctl nodes` TABLE
# does not show READY with at least POOL warm sandboxes, what it shows instead. The
# columns are found by their headers.
not_ready() {
	awk -v want="${node_ids[*]}" -v pool="$2" '
		NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
		{ state[$col["NODE"]] = $col["STATE"]; warm[$col["NODE"]] = $col["WARM"] }
		END {
			n = split(want, ids, " ")
			for (i = 1; i <= n; i++) {
				id = ids[i]
				if (!(id in state)) printf "%s is not registered; ", id
				else if (state[id] != "READY") printf "%s is %s; ", id, state[id]
				else if (warm[id] + 0 < pool + 0) printf "%s has %s of %s warm; ", id, warm[id], pool
			}
		}' <<<"$1"
}

# wait_ready POOL waits until every node of GSS_NODES is READY with POOL warm sandboxes,
# failing early when a unit has ended (a node agent that refused to start).
wait_ready() {
	local pool=$1 start=$SECONDS table missing unit units=("$sched_unit") i
	for ((i = 0; i < ${#node_ids[@]}; i++)); do
		units+=("gss-node-${node_ids[i]}.service")
	done
	say "waiting up to ${up_timeout}s for ${#node_ids[@]} nodes to be READY with $pool warm sandboxes each"
	while :; do
		for unit in "${units[@]}"; do
			if ! unit_running "$unit"; then
				say "$unit is not running; the last lines of its log:"
				sudo journalctl --unit="$unit" --lines=20 --no-pager >&2 || true
				die "$unit ended (journalctl -u $unit has its whole log; scripts/cluster.sh down stops the rest)"
			fi
		done
		if table=$("$gssctl" --scheduler "$sched_addr" nodes 2>&1); then
			missing=$(not_ready "$table" "$pool")
		else
			missing="the scheduler does not answer: $table"
		fi
		if [[ -z $missing ]]; then
			say "every node is READY with its warm pool full after $((SECONDS - start))s"
			echo "$table" >&2
			return 0
		fi
		if ((SECONDS - start >= up_timeout)); then
			echo "$table" >&2
			die "not ready after ${up_timeout}s: ${missing%; } (the cluster is left running: journalctl -u gss-node-<id> has a node's log, scripts/cluster.sh down stops it)"
		fi
		sleep 1
	done
}

up() {
	need_linux
	parse_nodes
	local cpu mem max pool b i
	cpu=$(int_env GSS_NODE_CPU 4000 1)
	mem=$(int_env GSS_NODE_MEM 2048 1)
	max=$(int_env GSS_NODE_MAX 16 1)
	pool=$(int_env GSS_POOL 2 0)
	for b in scheduler nodeagent gssctl; do
		if [[ ! -x $repo/bin/$b ]]; then
			die "$repo/bin/$b not found: run make build first"
		fi
	done
	local sched_args=() env_args=()
	read -ra sched_args <<<"${GSS_SCHED_ARGS:-}"
	if [[ -n ${OTEL_EXPORTER_OTLP_ENDPOINT:-} ]]; then
		env_args=(--setenv="OTEL_EXPORTER_OTLP_ENDPOINT=$OTEL_EXPORTER_OTLP_ENDPOINT")
	fi

	# I = -1 is the scheduler, then the nodes. Every running unit is checked before
	# anything starts. Agents retry registering until the scheduler answers, so they
	# need not wait for it.
	local unit unit_opts=() unit_cmd=()
	for ((i = -1; i < ${#node_ids[@]}; i++)); do
		unit_spec "$i"
		check_unit
	done
	for ((i = -1; i < ${#node_ids[@]}; i++)); do
		unit_spec "$i"
		start_unit
	done
	wait_ready "$pool"
}

# The leftovers of nodes whose runsc-root lock another process holds (a node agent
# running outside systemd) are not touched; find_held fills held with their IDs.
held=" "

is_held() { [[ $held == *" $1 "* ]]; }

find_held() {
	held=" "
	local lock id pid
	while read -r lock; do
		id=${lock##*/}
		id=${id%.lock}
		[[ $id =~ $node_id_re ]] || continue
		if ! sudo flock -n "$lock" true; then
			held+="$id "
			pid=$(sudo cat "$lock" 2>/dev/null || true)
			say "node $id: $lock is held (pid ${pid:-unknown}), by a node agent outside systemd; its cgroups, namespaces, nft table and runsc root are left in place"
		fi
	done < <(sudo find "$run_dir" -mindepth 1 -maxdepth 1 -name '*.lock' -type f 2>/dev/null)
}

# remove_cgroups kills whatever is left in each node's cgroup subtree and removes the
# node's cgroups, /sys/fs/cgroup/gss/<node> and below, deepest first.
remove_cgroups() {
	[[ -d $cgroup_dir ]] || return 0
	local d id try left dirs=()
	for d in "$cgroup_dir"/*/; do
		[[ -d $d ]] || continue
		d=${d%/}
		id=${d##*/}
		if is_held "$id"; then
			continue
		fi
		dirs+=("$d")
		if [[ -e $d/cgroup.kill ]]; then
			echo 1 | sudo tee "$d/cgroup.kill" >/dev/null || true
		fi
	done
	[[ ${#dirs[@]} -gt 0 ]] || return 0
	# A cgroup stays busy for a moment after its last process dies.
	for try in 1 2 3 4 5 6 7 8 9 10; do
		sudo find "${dirs[@]}" -depth -type d -exec rmdir {} + 2>/dev/null || true
		left=$(find "${dirs[@]}" -type d 2>/dev/null || true)
		if [[ -z $left ]]; then
			return 0
		fi
		sleep 0.5
	done
	say "cgroups left in place after $try tries:"
	echo "$left" >&2
	return 1
}

# remove_netns deletes the sandbox namespaces gss-<node>-<8 hex>; that also removes
# their veth pairs.
remove_netns() {
	local ns rest node rc=0
	while read -r ns rest; do
		[[ $ns =~ $netns_re ]] || continue
		node=${ns#gss-}
		node=${node%-*}
		if is_held "$node"; then
			continue
		fi
		sudo ip netns delete "$ns" || rc=1
	done < <(sudo ip netns list 2>/dev/null || true)
	return $rc
}

# remove_nft_tables deletes the tables inet gss_<node with - as _>.
remove_nft_tables() {
	command -v nft >/dev/null 2>&1 || return 0
	local kw family name rest node rc=0
	while read -r kw family name rest; do
		[[ $kw == table && $family == inet && $name =~ $table_re ]] || continue
		node=${name#gss_}
		node=${node//_/-}
		if is_held "$node"; then
			continue
		fi
		sudo nft delete table inet "$name" || rc=1
	done < <(sudo nft list tables 2>/dev/null || true)
	return $rc
}

# remove_run_dirs removes the runsc roots /run/gss/<node> and their locks
# /run/gss/<node>.lock. runsc leaves a bind-mounted null-netns in a root, so the mounts
# below /run/gss go first; rm stays on one file system in case one did not.
remove_run_dirs() {
	sudo test -d "$run_dir" || return 0
	local m rel p name rc=0
	while read -r m; do
		[[ -n $m ]] || continue
		rel=${m#"$run_dir"/}
		if is_held "${rel%%/*}"; then
			continue
		fi
		sudo umount "$m" || rc=1
	done < <(findmnt -rn -o TARGET 2>/dev/null | awk -v p="$run_dir/" 'index($0, p) == 1' | sort -r)
	while read -r p; do
		name=${p##*/}
		if [[ $name =~ $node_id_re || ($name == *.lock && ${name%.lock} =~ $node_id_re) ]]; then
			if is_held "${name%.lock}"; then
				continue
			fi
			sudo rm -rf --one-file-system -- "$p" || rc=1
		else
			say "left $p in place: not a node's runsc root or lock"
		fi
	done < <(sudo find "$run_dir" -mindepth 1 -maxdepth 1)
	return $rc
}

# remove_leftovers removes what stopped agents left, node by node, except the nodes
# whose lock is held. It fails when a node was skipped or something could not go.
remove_leftovers() {
	local rc=0
	find_held
	remove_cgroups || rc=1
	remove_netns || rc=1
	remove_nft_tables || rc=1
	remove_run_dirs || rc=1
	if [[ $held != " " ]]; then
		rc=1
	fi
	return $rc
}

down() {
	need_linux
	local unit active sub nodes=() sched=() failed=0
	while read -r unit active sub; do
		[[ -n $unit ]] || continue
		if [[ $unit == "$sched_unit" ]]; then
			sched=("$unit")
		else
			nodes+=("$unit")
		fi
	done < <(unit_lines || true)
	# The scheduler first, so that it does not reschedule the sandboxes of nodes that
	# are stopping; then the nodes together. Each agent destroys its own sandboxes.
	if [[ ${#sched[@]} -gt 0 ]]; then
		say "stopping $sched_unit"
		sudo systemctl stop "$sched_unit" || failed=1
	fi
	if [[ ${#nodes[@]} -gt 0 ]]; then
		say "stopping ${nodes[*]}"
		sudo systemctl stop "${nodes[@]}" || failed=1
	fi
	if [[ ${#sched[@]} -gt 0 || ${#nodes[@]} -gt 0 ]]; then
		sudo systemctl reset-failed ${sched[@]+"${sched[@]}"} ${nodes[@]+"${nodes[@]}"} >/dev/null 2>&1 || true
	fi
	# What is left belongs to no running agent only once every unit is gone.
	local left
	left=$(unit_lines | awk '$2 != "inactive" && $2 != "failed" { print $1 }' || true)
	if [[ -n $left ]]; then
		die "still running after the stop, so no leftovers were removed: ${left//$'\n'/ }"
	fi
	remove_leftovers || failed=1
	if [[ $failed -ne 0 ]]; then
		die "down did not finish cleanly (see above)"
	fi
	say "down: no gss units, cgroups, namespaces, nft tables or runsc roots are left"
}

status() {
	need_linux
	local lines
	lines=$(unit_lines || true)
	if [[ -z $lines ]]; then
		echo "no gss units are running"
		return 0
	fi
	echo "units:"
	awk '{ printf "  %-28s %s %s\n", $1, $2, $3 }' <<<"$lines"
	if ! unit_running "$sched_unit"; then
		echo "the scheduler is not running"
		return 0
	fi
	if [[ ! -x $gssctl ]]; then
		die "$gssctl not found: run make build first"
	fi
	echo
	"$gssctl" --scheduler "$sched_addr" nodes
}

# kill_nodes ID... sends SIGKILL to the node agents first, all at once, and then kills
# each node's cgroup subtree, so that its sandboxes die with it (spec D3).
kill_nodes() {
	local id units=()
	for id in "$@"; do
		if ! unit_running "gss-node-$id.service"; then
			die "node $id is not running (no unit gss-node-$id)"
		fi
		units+=("gss-node-$id.service")
	done
	sudo systemctl kill --signal=KILL "${units[@]}"
	for id in "$@"; do
		if [[ -e $cgroup_dir/$id/cgroup.kill ]]; then
			echo 1 | sudo tee "$cgroup_dir/$id/cgroup.kill" >/dev/null
		fi
	done
	say "killed node(s) $* and their sandboxes"
}

kill_node() {
	need_linux
	if [[ ! $1 =~ $node_id_re ]]; then
		die "\"$1\" is not a node ID"
	fi
	kill_nodes "$1"
}

kill_group() {
	need_linux
	parse_nodes
	local i ids=()
	for ((i = 0; i < ${#node_ids[@]}; i++)); do
		if [[ ${node_groups[i]} == "$1" ]]; then
			ids+=("${node_ids[i]}")
		fi
	done
	if [[ ${#ids[@]} -eq 0 ]]; then
		die "no node of group \"$1\" in GSS_NODES"
	fi
	kill_nodes "${ids[@]}"
}

case "${1:-}" in
up | down | status)
	[[ $# -eq 1 ]] || usage
	"$1"
	;;
kill-node)
	[[ $# -eq 2 ]] || usage
	kill_node "$2"
	;;
kill-group)
	[[ $# -eq 2 ]] || usage
	kill_group "$2"
	;;
*)
	usage
	;;
esac
