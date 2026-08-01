#!/usr/bin/env python3
"""Verify Linux hardening from emitted commands and the final ELF executable."""

from __future__ import annotations

import argparse
import json
import pathlib
import shlex
import shutil
import subprocess
import sys


def fail(message: str) -> None:
    raise RuntimeError(message)


def require_token(tokens: list[str], token: str, context: str) -> None:
    if token not in tokens:
        fail(f"{context} is missing {token}")


def compile_tokens(build_dir: pathlib.Path) -> list[str]:
    database = json.loads((build_dir / "compile_commands.json").read_text(encoding="utf-8"))
    matches = [entry for entry in database if entry["file"].endswith("/src/server/daemon_main.cpp")]
    if len(matches) != 1:
        fail(f"expected one daemon_main.cpp command, found {len(matches)}")
    entry = matches[0]
    return entry.get("arguments") or shlex.split(entry["command"])


def link_tokens(build_dir: pathlib.Path) -> list[str]:
    ninja = shutil.which("ninja")
    if ninja is None:
        fail("ninja is required to inspect the generated link command")
    output = subprocess.check_output(
        [ninja, "-C", str(build_dir), "-t", "commands", "glyphastored"], text=True
    )
    links = [line for line in output.splitlines() if " -o glyphastored " in f" {line} "]
    if len(links) != 1:
        fail(f"expected one glyphastored link command, found {len(links)}")
    return shlex.split(links[0])


def has_linker_option(tokens: list[str], option: str) -> bool:
    joined = " ".join(tokens)
    if (
        option in tokens
        or f"-Wl,{option}" in tokens
        or f"-Xlinker {option}" in joined
        or f"LINKER:{option}" in tokens
    ):
        return True
    # CMake/Ninja may emit `-Wl,-z -Wl,relro` or `-Xlinker -z -Xlinker relro`.
    if option.startswith("-z,"):
        value = option.split(",", 1)[1]
        compact = joined.replace(" ", "")
        if f"-z,{value}" in joined or f"-z{value}" in compact:
            return True
        if f"-Wl,-z,-Wl,{value}" in compact or f"-Wl,-z,{value}" in joined:
            return True
        if f"-Xlinker-z-Xlinker{value}" in compact:
            return True
        for index, token in enumerate(tokens[:-1]):
            nxt = tokens[index + 1]
            if token in ("-z", f"-Wl,-z") and nxt in (value, f"-Wl,{value}"):
                return True
            if token == "-Xlinker" and nxt == "z" and index + 3 < len(tokens):
                if tokens[index + 2] == "-Xlinker" and tokens[index + 3] == value:
                    return True
    return False


def verify_commands(build_dir: pathlib.Path) -> None:
    compile = compile_tokens(build_dir)
    require_token(compile, "-std=c++23", "daemon compile command")
    if any(token.startswith("-std=gnu++") for token in compile):
        fail("daemon compile command enables GNU language extensions")
    require_token(compile, "-Werror", "strict daemon compile command")
    require_token(compile, "-Wpedantic", "strict daemon compile command")
    require_token(compile, "-fstack-protector-strong", "daemon compile command")
    require_token(compile, "-fPIE", "daemon compile command")
    require_token(compile, "-D_FORTIFY_SOURCE=3", "optimized daemon compile command")
    if not any(token.startswith("-O") and token != "-O0" for token in compile):
        fail("fortified daemon command is not optimized")

    link = link_tokens(build_dir)
    for option in ("-pie", "-z,relro", "-z,now"):
        if not has_linker_option(link, option):
            fail(f"glyphastored link command is missing {option}")


def readelf(binary: pathlib.Path, *options: str) -> str:
    executable = shutil.which("readelf")
    if executable is None:
        fail("readelf is required to inspect the final ELF")
    return subprocess.check_output([executable, *options, str(binary)], text=True)


def verify_elf(binary: pathlib.Path) -> None:
    header = readelf(binary, "-h")
    if "Type:" not in header or "DYN" not in header:
        fail("glyphastored is not an ELF position-independent executable")
    if "GNU_RELRO" not in readelf(binary, "-l"):
        fail("glyphastored has no PT_GNU_RELRO segment")
    dynamic = readelf(binary, "-d")
    if "BIND_NOW" not in dynamic and not any(
        "FLAGS" in line and "NOW" in line for line in dynamic.splitlines()
    ):
        fail("glyphastored does not request immediate dynamic binding")
    if "__stack_chk_fail" not in readelf(binary, "-Ws"):
        fail("glyphastored has no observable stack-protector dependency")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("build_dir", type=pathlib.Path)
    args = parser.parse_args()
    build_dir = args.build_dir.resolve()
    verify_commands(build_dir)
    verify_elf(build_dir / "glyphastored")
    print("hardening verification passed: ISO C++23, emitted flags, and ELF properties")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, subprocess.CalledProcessError, RuntimeError, ValueError) as error:
        print(f"hardening verification failed: {error}", file=sys.stderr)
        sys.exit(1)
