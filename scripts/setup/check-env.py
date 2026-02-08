# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "cxc-toolkit>=1.1.2",
#     "tabulate>=0.9.0",
# ]
# ///

import re
import shutil

from tabulate import tabulate
import cxc_toolkit


class CLITool:
    def __init__(self, name, description, min_version, version_regex):
        self.name = name
        self.description = description
        self.min_version = min_version
        self.version_regex = version_regex

    def get_installed_info(self):
        output, _ = cxc_toolkit.exec.run_command(
            f"{self.name} --version",
            ignore_failure=True,
            slient=True,
        )

        match = re.search(self.version_regex, output)
        version = match.group(1) if match else "unknown"

        location = shutil.which(self.name)
        if location:
            return f"{version} ({location})"
        return version


class SystemLibrary:
    def __init__(self, name, description, min_version):
        self.name = name
        self.description = description
        self.min_version = min_version
        self.version_regex = r"^Version:\s*(\S+)"

    def get_installed_info(self):
        # Caution: use "dpkg -s" instead of "apt show" since the latter also
        # shows the information of uninstalled packages.
        output, _ = cxc_toolkit.exec.run_command(
            f"dpkg -s {self.name}",
            ignore_failure=True,
            slient=True,
        )

        match = re.search(self.version_regex, output, flags=re.MULTILINE)
        return match.group(1) if match else "unknown"


class ToolList:
    def __init__(self):
        self.tools = []

    def add_cli_tool(self, name, description, min_version, version_regex):
        tool = CLITool(name, description, min_version, version_regex)
        self.tools.append(tool)

    def add_system_library(self, name, description, min_version):
        library = SystemLibrary(name, description, min_version)
        self.tools.append(library)

    def display(self):
        table_data = [
            [
                tool.name,
                tool.description,
                tool.min_version,
                tool.get_installed_info(),
            ]
            for tool in self.tools
        ]
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
    build_tools = ToolList()
    build_tools.add_cli_tool(
        "cmake", "build-system generator", "3.15", r"cmake\s+version\s+([0-9.]+)"
    )
    build_tools.add_cli_tool("ninja", "primary build-system", "1.10", r"([0-9.]+)")
    build_tools.add_cli_tool(
        "make", "build-system", "4.0", r"GNU Make\s+([0-9.]+)"
    )
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
    # build_tools.add_system_library("libdw-dev", "...", "0.1.3")
    # build_tools.add_system_library("binutils-dev", "...", "2.40")

    print("Tools Required for Building:")
    build_tools.display()


if __name__ == "__main__":
    check_env()
