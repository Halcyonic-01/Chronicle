#!/usr/bin/env bash
# Leader-kill drill, local kind only. A dedicated zero-replica Deployment has its
# environment changed at a steady pace; the leader is killed with no chance to
# release its lease; afterwards every change must still be accounted for.
#
# What is checked (see internal/heal/README.md, "P2"):
#   - takeover time: how long until another replica holds the lease;
#   - the recorded template moves lead, unbroken, from the first state to the
#     last. A change nobody saw is summarised by one catch-up event, so changes
#     recorded individually may be fewer than changes made, but none may vanish;
#   - the same move is not recorded twice and no event row is stored twice.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/kube-context.sh

# DRILL_MODE: delete (force-deleted pod, may release its lease), kill9 (SIGKILL: no
# release), freeze (SIGSTOP past the lease, then SIGCONT: a stale leader wakes up).
MODE="${DRILL_MODE:-delete}"
FREEZE_FOR="${DRILL_FREEZE_FOR:-40}"
TOGGLES="${DRILL_TOGGLES:-30}"   # environment changes to make
EVERY="${DRILL_EVERY:-3}"        # seconds between them
KILL_AT="${DRILL_KILL_AT:-10}"   # kill the leader after this many
NS=default NAME=drill-target
holder() { kubectl -n chronicle get lease chronicle-leader -o jsonpath='{.spec.holderIdentity}'; }
# The host PID of the leader's process, found through the kind node's runtime.
leader_pid() {
  local node cid
  node="$(kubectl -n chronicle get pod "$1" -o jsonpath='{.spec.nodeName}')"
  cid="$(kubectl -n chronicle get pod "$1" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's#.*://##')"
  docker exec "$node" crictl inspect "$cid" | python3 -c 'import json,sys; print(json.load(sys.stdin)["info"]["pid"])'
  echo "$node" >&2
}
sql() { kubectl -n chronicle exec postgres-postgresql-0 -- env PGPASSWORD=postgres psql -U postgres -At -F '|' -c "$1"; }

kubectl -n "$NS" delete deployment "$NAME" --ignore-not-found >/dev/null
kubectl -n "$NS" create deployment "$NAME" --image=registry.k8s.io/pause:3.10 --replicas=0 >/dev/null
sleep 8   # let the first sight of it be recorded
start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
leader="$(holder)"; echo "leader: $leader"

for i in $(seq 1 "$TOGGLES"); do
  kubectl -n "$NS" set env deployment/"$NAME" "DRILL=$i" >/dev/null
  if [ "$i" -eq "$KILL_AT" ]; then
    echo "$MODE: disrupting $leader at change $i"
    killed="$(date +%s)"
    case "$MODE" in
      delete) kubectl -n chronicle delete pod "$leader" --grace-period=0 --force >/dev/null 2>&1 & ;;
      kill9|freeze)
        pid="$(leader_pid "$leader" 2>/tmp/drill-node)"; node="$(cat /tmp/drill-node)"
        if [ "$MODE" = kill9 ]; then docker exec "$node" kill -9 "$pid"
        else
          docker exec "$node" kill -STOP "$pid"
          ( sleep "$FREEZE_FOR"; docker exec "$node" kill -CONT "$pid"; echo "resumed the stale leader after ${FREEZE_FOR}s" ) &
        fi ;;
      *) echo "unknown DRILL_MODE $MODE" >&2; exit 2 ;;
    esac
    ( for _ in $(seq 90); do
        [ "$(holder)" != "$leader" ] && [ -n "$(holder)" ] && { echo "takeover: $(( $(date +%s) - killed ))s, new leader $(holder)"; exit 0; }
        sleep 1
      done
      echo "no other replica took over within 90s (the same replica kept or regained the lease)" ) &
  fi
  sleep "$EVERY"
done
wait
sleep 30   # the new leader's catch-up
# One more change: its "from" must be the last state recorded, or a state was skipped.
kubectl -n "$NS" set env deployment/"$NAME" "DRILL=final" >/dev/null
sleep 20

rows="$(sql "SELECT occurred_at, coalesce(payload->>'from_template_hash',''), coalesce(payload->>'to_template_hash',''), coalesce(payload->>'missed','') FROM events WHERE entity_name='$NAME' AND type='config_change' AND occurred_at >= '$start' ORDER BY occurred_at")"
echo "$rows" | python3 -c '
import sys
rows=[l.split("|") for l in sys.stdin.read().strip().splitlines()]
changes=int(sys.argv[1])+1
live=[r for r in rows if r[3]!="true"]
catch=[r for r in rows if r[3]=="true"]
# Every recorded move (a live one, or a catch-up summarising several) is an edge
# between two template hashes. No state was skipped if the final one is reachable
# from the first.
edges={}
for r in rows:
    if r[1] and r[2]: edges.setdefault(r[1],set()).add(r[2])
seen,todo=set(),[live[0][1]] if live else []
while todo:
    h=todo.pop()
    if h not in seen: seen.add(h); todo+=list(edges.get(h,()))
reached=bool(live) and live[-1][2] in seen
dup=len(live)-len({(r[1],r[2]) for r in live})
print(f"changes made: {changes}  recorded individually: {len(live)}  catch-up summaries: {len(catch)}  not recorded individually: {changes-len(live)}")
print(f"first state reaches the last through recorded moves: {reached}   same move recorded twice: {dup}")
sys.exit(0 if reached and dup==0 else 1)' "$TOGGLES"
echo "duplicates: $(sql "SELECT count(*) - count(distinct id) FROM events WHERE entity_name='$NAME'")"
kubectl -n "$NS" delete deployment "$NAME" --ignore-not-found >/dev/null
