.PHONY: bootstrap xcode configure build test asan tsan benchmark format clean

bootstrap:
	./scripts/bootstrap-macos.sh

xcode:
	./scripts/open-xcode.sh

configure:
	./scripts/dev.sh configure

build:
	./scripts/dev.sh build

test:
	./scripts/dev.sh test

asan:
	./scripts/dev.sh asan

tsan:
	./scripts/dev.sh tsan

benchmark:
	./scripts/dev.sh benchmark

format:
	./scripts/dev.sh format

clean:
	./scripts/dev.sh clean
