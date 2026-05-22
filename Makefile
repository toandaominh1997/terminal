PORT ?= 8000
HOST ?= 0.0.0.0
BIN  := xterm-web
PID  := .xterm-web.pid
LOG  := .xterm-web.log

.PHONY: build run start stop restart status logs clean

build:
	cargo build --release

run: build
	XTERM_HOST=$(HOST) XTERM_PORT=$(PORT) ./target/release/$(BIN)

start: build
	@if [ -f $(PID) ] && kill -0 `cat $(PID)` 2>/dev/null; then \
		echo "already running (pid=`cat $(PID)`)"; exit 1; \
	fi
	@XTERM_HOST=$(HOST) XTERM_PORT=$(PORT) nohup ./target/release/$(BIN) >$(LOG) 2>&1 & echo $$! > $(PID)
	@sleep 1
	@echo "started $(BIN) on $(HOST):$(PORT) (pid=`cat $(PID)`, log=$(LOG))"

stop:
	@if [ -f $(PID) ]; then \
		PID=`cat $(PID)`; \
		if kill -0 $$PID 2>/dev/null; then kill $$PID && echo "stopped pid=$$PID"; \
		else echo "stale pidfile"; fi; \
		rm -f $(PID); \
	else echo "not running"; fi

restart: stop start

status:
	@if [ -f $(PID) ] && kill -0 `cat $(PID)` 2>/dev/null; then \
		PID=`cat $(PID)`; \
		ADDR=`lsof -nP -iTCP -sTCP:LISTEN -a -p $$PID 2>/dev/null | awk 'NR>1 {print $$9; exit}'`; \
		echo "running (pid=$$PID, listening=$${ADDR:-unknown})"; \
	else echo "not running"; fi

logs:
	@tail -f $(LOG)

clean:
	cargo clean
	rm -f $(PID) $(LOG)
