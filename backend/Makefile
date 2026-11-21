# WSL2: systemd user session can't start, so /run/user/1000/bus doesn't exist.
# If conmon/crun sees DBUS_SESSION_BUS_ADDRESS it tries the systemd path and fails
# with "sd-bus call: Access denied" even though cgroup_manager is cgroupfs.
unexport DBUS_SESSION_BUS_ADDRESS
# WSL2: netavark launches aardvark-dns via `systemd-run --user` when it finds
# systemd-run in PATH, but there is no user session here. Prepend a shim that
# strips systemd-run flags and execs directly.
export PATH := $(HOME)/.local/share/podman-wsl-shims:$(PATH)
# WSL2: without a systemd user session, podman cannot register healthcheck
# timers over the user bus ("unable to get systemd connection to add
# healthchecks"). DISABLE_HC_SYSTEMD is podman's documented escape hatch.
export DISABLE_HC_SYSTEMD := true

.PHONY: proto build up down podman-api migrate seed-scheduler seed-baseline test logs clean

SERVICES := gateway user event booking notification payment saga scheduler

proto:
	python3 -B scripts/check_proto.py --write

build:
	@for svc in $(SERVICES); do \
		echo "Building $$svc..."; \
		cd services/$$svc && go build -o ../../bin/$$svc ./cmd && cd ../..; \
	done

up: podman-api
	@bash scripts/podman-up.sh

# Expose the podman API over TCP for Podman Desktop on Windows. The rootless
# podman.socket cannot be socket-activated here (no systemd user session on
# WSL2), so Podman Desktop cannot reach this distro without it.
# Windows side: podman system connection add ubuntu-wsl tcp://localhost:8090
podman-api:
	@curl -s http://127.0.0.1:8090/_ping >/dev/null 2>&1 \
		|| (nohup podman system service --time=0 tcp://127.0.0.1:8090 >/tmp/podman-api.log 2>&1 & \
			sleep 2 && echo "podman API listening on tcp://127.0.0.1:8090")

down:
	podman compose down

migrate:
	@bash scripts/migrate.sh

seed-scheduler:
	@bash scripts/seed-scheduler.sh

seed-baseline:
	@test -n "$$EVENT_IDS" || (echo "EVENT_IDS=<uuid>[,<uuid>...] is required (export it before make)" >&2; exit 2)
	@bash scripts/seed-baseline.sh

test:
	@for svc in $(SERVICES); do \
		echo "Testing $$svc..."; \
		cd services/$$svc && go test ./... -v && cd ../..; \
	done

logs:
	podman compose logs -f

clean:
	rm -rf bin/
