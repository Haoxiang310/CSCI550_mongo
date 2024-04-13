"""
Signs all of the known testing binaries with insecure development entitlements.

Specifically the `Get Task Allow` is what we are looking for.
Adding the `Get Task Allow` entitlement allows us to attach to
the mongo processes and get core dumps/debug in any way we need.
You can view some more documentation on this topic here:
https://developer.apple.com/documentation/bundleresources/entitlements/com_apple_security_cs_debugger#discussion
"""

import os
import subprocess
import sys


def main():
    """Signs all of the known testing binaries with insecure development entitlements."""
    if sys.platform != "darwin":
        print("Non-macos system detected, do not need to sign binaries.")
        sys.exit(0)

    build_bin_dir = os.path.join("build", "install", "bin")
    binary_directories = ["/data/multiversion", os.path.join("dist-test", "bin"), build_bin_dir]
    entitlements_file = os.path.abspath(os.path.join("etc", "macos_dev_entitlements.xml"))
    assert os.path.exists(entitlements_file), f"{entitlements_file} does not exist"

    for binary_dir in binary_directories:
        if not os.path.exists(binary_dir):
            continue

        for binary in os.listdir(binary_dir):
            binary_path = os.path.join(binary_dir, binary)
            if not os.path.isfile(binary_path):
                continue

            print(f"Signing {binary}")
            subprocess.run([
                "/usr/bin/codesign", "-s", "-", "-f", "--entitlements", entitlements_file,
                binary_path
            ], check=True)


if __name__ == '__main__':
    main()
