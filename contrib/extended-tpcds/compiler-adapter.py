#!/usr/bin/env python3
import os, subprocess, sys
# Runtime initialization only: compile SQL and generated files pass through unchanged.
result = subprocess.run([os.environ['EXTENDED_TPCDS_NATIVE_CLI'], '-unsigned', '-init', os.environ['EXTENDED_TPCDS_COMPILER_INIT'], *sys.argv[1:]], input=sys.stdin.buffer.read())
sys.exit(result.returncode)
