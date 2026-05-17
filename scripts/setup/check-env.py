# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "cxc-toolkit>=1.1.2",
#     "tabulate>=0.9.0",
# ]
# ///

import os
import re
import shutil
import socket
import sys

import cxc_toolkit
from tabulate import tabulate

VAGRANT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "small-db-jepsen", "vagrant")
)
VAGRANT_VMS = ["america", "europe", "asia"]
NODES_FILE = os.path.join(VAGRANT_DIR, "nodes")


class CLITool:
    def __init__(self, name, description, min_version, version_regex):
        self.name = name
        self.description = description
        self.min_version = min_version
        self.version_regex = version_regex
        self._info = None
        self._satisfied = None

    def _probe(self):
        if self._info is not None:
            return
        output, _ = cxc_toolkit.exec.run_command(
            f"{self.name} --version",
            ignore_failure=True,
            slient=True,
        )

        match = re.search(self.version_regex, output)
        version = match.group(1) if match else "unknown"

        location = shutil.which(self.name)
        self._satisfied = location is not None
        self._info = f"{version} ({location})" if location else version

    def get_installed_info(self):
        self._probe()
        return self._info

    def is_satisfied(self):
        self._probe()
        return self._satisfied


class SystemLibrary:
    def __init__(self, name, description, min_version):
        self.name = name
        self.description = description
        self.min_version = min_version
        self.version_regex = r"^Version:\s*(\S+)"
        self._info = None
        self._satisfied = None

    def _probe(self):
        if self._info is not None:
            return
        # Caution: use "dpkg -s" instead of "apt show" since the latter also
        # shows the information of uninstalled packages.
        output, _ = cxc_toolkit.exec.run_command(
            f"dpkg -s {self.name}",
            ignore_failure=True,
            slient=True,
        )

        match = re.search(self.version_regex, output, flags=re.MULTILINE)
        self._satisfied = match is not None
        self._info = match.group(1) if match else "unknown"

    def get_installed_info(self):
        self._probe()
        return self._info

    def is_satisfied(self):
        self._probe()
        return self._satisfied


class ToolList:
    def __init__(self):
        self.tools = []

    def add_cli_tool(self, name, description, min_version, version_regex):
        tool = CLITool(name, description, min_version, version_regex)
        self.tools.append(tool)

    def add_system_library(self, name, description, min_version):
        library = SystemLibrary(name, description, min_version)
        self.tools.append(library)

    def display(self, failures):
        table_data = []
        for tool in self.tools:
            info = tool.get_installed_info()
            table_data.append([tool.name, tool.description, tool.min_version, info])
            if not tool.is_satisfied():
                failures.append(f"{tool.name} not installed")
        print(
            tabulate(
                table_data,
                headers=[
                    "Tool",
                    "Description",
                    "Minimum Version",
                    "Installed",
                ],
                tablefmt="grid",
                disable_numparse=True,
            )
        )


def check_env():
    failures = []

    build_tools = ToolList()
    build_tools.add_cli_tool(
        "cmake", "build-system generator", "3.15", r"cmake\s+version\s+([0-9.]+)"
    )
    build_tools.add_cli_tool("ninja", "primary build-system", "1.10", r"([0-9.]+)")
    build_tools.add_cli_tool("make", "build-system", "4.0", r"GNU Make\s+([0-9.]+)")
    build_tools.add_cli_tool(
        "clang-18", "C++ compiler", "18.0", r"clang version\s+([0-9.]+)"
    )
    build_tools.add_system_library("clang-tools-18", "C++ compiler tools", "18.0")
    build_tools.add_system_library("libboost-all-dev", "required by arrow", "1.81")
    build_tools.add_system_library("libpq-dev", "PostgreSQL client C library", "14")
    build_tools.add_system_library(
        "libpqxx-dev", "PostgreSQL client C++ library", "7.6.0"
    )
    build_tools.add_system_library("uuid-dev", "UUID library", "2.36.0")

    print("Tools Required for Building:")
    build_tools.display(failures)

    format_tools = ToolList()
    format_tools.add_cli_tool(
        "clang-format-18",
        "C/C++ source formatter (used by run-clang-format.py)",
        "18.0",
        r"clang-format version\s+([0-9.]+)",
    )
    format_tools.add_cli_tool(
        "clang-tidy-18",
        "C/C++ static analyzer (used by run-clang-tidy.py)",
        "18.0",
        r"LLVM version\s+([0-9.]+)",
    )
    format_tools.add_cli_tool(
        "run-clang-tidy-18",
        "LLVM parallel runner for clang-tidy",
        "(ships with clang-tidy-18)",
        r"(?!x)x",
    )

    print("\nTools Required for Linting/Formatting:")
    format_tools.display(failures)

    jepsend_tools = ToolList()
    jepsend_tools.add_cli_tool(
        "vagrant", "virtual machine manager", "2.2.0", r"Vagrant\s+([0-9.]+)"
    )
    jepsend_tools.add_system_library("virtualbox", "virtual machine provider", "7.0")

    # install: go install github.com/guumaster/hostctl/cmd/hostctl@v1.1.4
    jepsend_tools.add_cli_tool(
        "hostctl", "manage /etc/hosts entries", "1.1.4", r"hostctl version (\S+)"
    )
    jepsend_tools.add_cli_tool(
        "lein", "build tool for Jepsen", "2.9.1", r"Leiningen\s+(\S+)"
    )
    jepsend_tools.add_cli_tool(
        "gnuplot", "plotting tool for Jepsen results", "6.0", r"gnuplot\s+([0-9.]+)"
    )

    print("\nTools Required for Jepsen Testing:")
    jepsend_tools.display(failures)

    print("\nKernel Modules Required by Vagrant:")
    output, _ = cxc_toolkit.exec.run_command("lsmod", slient=True)
    loaded_modules = {line.split()[0] for line in output.splitlines()}
    required_status = [
        ("kvm_amd", "KVM module for AMD CPUs", False, "sudo modprobe -r kvm_amd"),
        ("vboxdrv", "VirtualBox kernel module", True, "sudo modprobe vboxdrv"),
        ("vboxnetflt", "VirtualBox network filter module", True, "sudo modprobe vboxnetflt"),
        ("vboxnetadp", "VirtualBox network adapter module", True, "sudo modprobe vboxnetadp"),
    ]
    for module, description, should_be_loaded, fix_cmd in required_status:
        is_loaded = module in loaded_modules
        status = "loaded" if is_loaded else "not loaded"
        is_ok = should_be_loaded == is_loaded
        mark = "✓" if is_ok else "✗"

        message = f"- {mark} {module}: {description} - {status}"
        if should_be_loaded:
            message += " (should be loaded)"
        else:
            message += " (should be disabled)"
        print(message)
        if not is_ok:
            print(f"    fix: {fix_cmd}")
            failures.append(f"kernel module {module} state")

    print("\nVagrant VM Status:")
    output, _ = cxc_toolkit.exec.run_command(
        "vagrant status --machine-readable",
        work_dir=VAGRANT_DIR,
        ignore_failure=True,
        slient=True,
    )
    vm_state = {}
    for line in output.splitlines():
        parts = line.split(",")
        if len(parts) >= 4 and parts[2] == "state":
            vm_state[parts[1]] = parts[3]
    for vm in VAGRANT_VMS:
        state = vm_state.get(vm, "unknown")
        is_ok = state == "running"
        mark = "✓" if is_ok else "✗"
        print(f"- {mark} {vm}: {state} (should be running)")
        if not is_ok:
            print(f"    fix: (cd {VAGRANT_DIR} && vagrant up {vm})")
            failures.append(f"VM {vm} not running")

    print("\nVagrant Hostname Resolution (/etc/hosts via hostctl):")
    expected = {}
    if os.path.exists(NODES_FILE):
        with open(NODES_FILE) as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 2:
                    ip, name = parts
                    expected[name] = ip
    for vm in VAGRANT_VMS:
        want = expected.get(vm)
        try:
            got = socket.gethostbyname(vm)
        except OSError:
            got = None
        is_ok = want is not None and got == want
        mark = "✓" if is_ok else "✗"
        detail = f"{got or 'unresolved'}" + (f" (expected {want})" if want and got != want else "")
        print(f"- {mark} {vm}: {detail}")
        if not is_ok:
            print(
                f"    fix: (cd {VAGRANT_DIR} && sudo $(go env GOPATH)/bin/hostctl add small-db-jepsen --from ./nodes)"
            )
            failures.append(f"{vm} hostname does not resolve to expected IP")

    print("\nVagrant VMs Reachable (SSH port 22 on private IP):")
    for vm in VAGRANT_VMS:
        want = expected.get(vm)
        if not want:
            is_ok = False
            detail = "no IP in nodes file"
        else:
            try:
                with socket.create_connection((want, 22), timeout=3):
                    is_ok = True
                    detail = f"{want}:22 reachable"
            except OSError as e:
                is_ok = False
                detail = f"{want}:22 unreachable ({e.__class__.__name__})"
        mark = "✓" if is_ok else "✗"
        print(f"- {mark} {vm}: {detail}")
        if not is_ok:
            print(f"    fix: (cd {VAGRANT_DIR} && vagrant reload {vm})")
            failures.append(f"{vm} VM not reachable on SSH port")

    debug_tools = ToolList()
    debug_tools.add_cli_tool(
        "psql",
        "PostgreSQL command-line client",
        "14.0",
        r"psql\s+\(PostgreSQL\)\s+([0-9.]+)",
    )
    print("\nTools Required for Debugging:")
    debug_tools.display(failures)

    book_tools = ToolList()
    book_tools.add_cli_tool(
        "cargo", "Rust build tool (for mdbook)", "1.70", r"cargo\s+([0-9.]+)"
    )
    book_tools.add_cli_tool(
        "mdbook",
        "tool for building the book",
        "0.4.0",
        r"mdbook\s+v([0-9.]+)",
    )
    print("\nTools Required for Building the Book:")
    book_tools.display(failures)

    print()
    if failures:
        print(f"Unsatisfied conditions ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(-1)
    print("All checks satisfied.")


if __name__ == "__main__":
    check_env()
