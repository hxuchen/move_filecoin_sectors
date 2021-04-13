SHELL=/usr/bin/env bash

all: build
.PHONY: all

unexport GOFLAGS

BINS:=

ldflags=-X=move_sectors/build.CurrentCommit=+git.$(subst -,.,$(shell git describe --always --match=NeVeRmAtCh --dirty 2>/dev/null || git rev-parse --short HEAD 2>/dev/null))
ifneq ($(strip $(LDFLAGS)),)
	ldflags+=-extldflags=$(LDFLAGS)
endif

GOFLAGS+=-ldflags="-s -w $(ldflags)"

move_sectors:
	rm -f lotus-gateway
	go build $(GOFLAGS) -o move_sectors ./cmd
.PHONY: move_sectors
BINS+=move_sectors

build: move_sectors

.PHONY: build

install: install-move-sectors

install-move-sectors:
	install -C ./move_sectors /usr/local/bin/move_sectors


buildall: $(BINS)

clean:
	rm -rf $(CLEAN) $(BINS)
.PHONY: clean