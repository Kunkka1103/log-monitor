# Makefile for log-monitor

# Go build variables
GO ?= go
BINARY_NAME = log-monitor
SRC = main.go

# Default target
all: build

# Build the binary
build:
	$(GO) build -o $(BINARY_NAME) $(SRC)

# Clean the build
clean:
	rm -f $(BINARY_NAME)

.PHONY: all build clean