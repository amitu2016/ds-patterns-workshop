.PHONY: help build test test-java test-go demos clean check-anchors check-demos check-script script-index

help:
	@echo "litesystems — small, faithful models of real distributed systems"
	@echo ""
	@echo "  make demo-<id>   run one demo, e.g. make demo-2.2"
	@echo "  make demos       list every demo that exists"
	@echo "  make build       compile everything (Java + Go)"
	@echo "  make test        run every test (Java + Go)"
	@echo "  make test-java   Gradle tests only"
	@echo "  make test-go     kubelite tests only, never cached"
	@echo ""
	@echo "Full index with slide mapping: DEMOS.md"

# Every `SLIDE:` anchor must name a real slide in one of the decks. Anchors rot silently
# whenever a deck is edited, and a stale anchor breaks the slide<->code navigation that is
# the whole point. `SLIDE (TODO):` marks a slide that still has to be written.
DECKS := $(HOME)/work/MADSPlantUMLSteps/src/presentation/oreilly-design-patterns-distributed-systems-workshop.yaml \
         $(HOME)/work/MADSPlantUMLSteps/src/presentation/erasure-coding-reed-solomon.yaml

# Instructor-only: the decks live outside this repo, so a published copy has nothing to
# check against and says so rather than failing.
check-anchors:
	@if [ ! -f "$(firstword $(DECKS))" ]; then \
	  echo "slide decks not present - skipping anchor check"; exit 0; fi; \
	stale=$$(grep -rhoE 'SLIDE: .*' --include=*.java --include=*.go . \
	  | sed 's/SLIDE: //; s/ *(demo [0-9.]*) *$$//; s/ *$$//' | sort -u \
	  | while IFS= read -r t; do grep -qhF "title: \"$$t\"" $(DECKS) || echo "$$t"; done); \
	pending=$$(grep -rho 'SLIDE (TODO):' --include=*.java --include=*.go . | wc -l | tr -d ' '); \
	if [ -n "$$stale" ]; then echo "$$stale" | sed 's/^/  STALE ANCHOR: /'; else echo "all anchors resolve"; fi; \
	if [ "$$pending" != "0" ]; then echo "  ($$pending demo(s) reference slides not yet written)"; fi; \
	[ -z "$$stale" ]

# kubelite is Go, and a published slice may not include it. Skipping an absent module is
# fine; swallowing a failure of one that is present is not -- that was the old bug.
# Instructor-only, like check-anchors: the script lives under docs/, which publishWorkshop
# does not copy, so a published checkout says so rather than failing.
WPM ?= 130
FILL ?= 70
check-script:
	@if [ ! -f tools/check-script-timing.py ]; then \
	  echo "speaking script not present - skipping timing check"; exit 0; fi; \
	python3 tools/check-script-timing.py docs/script $(WPM) $(FILL)

check-demos:
	@python3 tools/check-demos-index.py

script-index:
	@if [ ! -f tools/script-index.py ]; then \
	  echo "speaking script not present - skipping"; exit 0; fi; \
	python3 tools/script-index.py docs/script

build:
	./gradlew build
	@if [ -d kubelite ]; then cd kubelite && go build ./...; \
	else echo "kubelite not in this checkout - skipping Go build"; fi

# `test` must fail when anything fails. It previously ran the Go side as
# `(go test ./... 2>/dev/null || true)`, which discarded the output *and* the exit
# status, so a red kubelite reported green here and nobody could see why.
test: test-java test-go

test-java:
	./gradlew test

# -count=1 defeats Go's test cache. Without it Go replays a previous result and a
# demo whose TRY IT constant you just changed prints the old behaviour -- the same
# reason the Gradle test task sets outputs.upToDateWhen { false }.
test-go:
	@if [ -d kubelite ]; then cd kubelite && go test -count=1 ./...; \
	else echo "kubelite not in this checkout - skipping Go tests"; fi

clean:
	./gradlew clean

# Demos are auto-discovered by class/func name, so no target needs maintaining.
#   Java: class Demo_2_2_EpochFencing        Go: func TestDemo_2_4_Scheduler
demos:
	@echo "Java:"
	@grep -rho --include=*.java 'class Demo_[0-9]*_[0-9]*_[A-Za-z0-9]*' . 2>/dev/null \
	  | sed 's/class Demo_/  /' | sort -u || true
	@echo "Go:"
	@grep -rho --include=*_test.go 'func TestDemo_[0-9]*_[0-9]*_[A-Za-z0-9]*' kubelite 2>/dev/null \
	  | sed 's/func TestDemo_/  /' | sort -u || true

demo-%:
	@id=$$(echo "$*" | tr '.' '_'); \
	jf=$$(grep -rl --include=*.java "class Demo_$${id}_" . 2>/dev/null); \
	gf=$$(grep -rl --include=*_test.go "func TestDemo_$${id}_" kubelite 2>/dev/null); \
	if [ -z "$$jf" ] && [ -z "$$gf" ]; then \
	  echo "No demo $* yet. See DEMOS.md for the backlog, or run: make demos"; exit 1; \
	fi; \
	if [ -n "$$jf" ]; then \
	  projs=$$(echo "$$jf" | sed 's|^\./||; s|/src/test/java/.*||; s|/|:|g' | sort -u); \
	  for p in $$projs; do \
	    echo "==> :$$p  --tests *Demo_$${id}_*"; \
	    ./gradlew ":$$p:test" --tests "*Demo_$${id}_*" || exit 1; \
	  done; \
	fi; \
	if [ -n "$$gf" ]; then \
	  pkgs=$$(echo "$$gf" | xargs -n1 dirname | sed 's|^kubelite/|./|' | sort -u); \
	  for pkg in $$pkgs; do \
	    echo "==> go test $$pkg -run TestDemo_$${id}_"; \
	    (cd kubelite && go test -v -count=1 -run "TestDemo_$${id}_" "$$pkg") || exit 1; \
	  done; \
	fi
