"""Prints a greeting, the Python version and the kernel release the program sees.

Run it with: bin/gssctl run -f examples/hello.py
Inside a sandbox the release is the one gVisor reports, not the VM's kernel.
"""

import platform
import sys

print("hello from a gss sandbox")
print("python", sys.version.split()[0])
print("kernel release", platform.release())
