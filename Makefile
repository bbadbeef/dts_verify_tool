VERSION=1.0.0
SRC=./
CURDIR=$(shell pwd)
BINDIR=$(CURDIR)/bin
LDFLAGS += -X "version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "version.GitSHA=$(shell git rev-parse HEAD)"
Target=dts_verify_tool
TargetOS=linux
#TargetOS=darwin
TargetARCH=amd64

server:
	GOOS=$(TargetOS) GOARCH=$(TargetARCH) go build -ldflags '$(LDFLAGS)' -o $(BINDIR)/$(Target) $(SRC)/cmd/
clean:
	rm -rf $(BINDIR)
