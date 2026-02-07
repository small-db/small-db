#!/usr/bin/env python

import re
import shutil
import subprocess

from tabulate import tabulate


class CLITool:
    def __init__(self, name, min_version, version_regex=None):
        self.name = name
        self.min_version = min_version
        self.version_regex = version_regex

    def get_installed_version(self):
        try:
            result = subprocess.run(
                [self.name, "--version"],
                check=False,
                text=True,
                capture_output=True,
            )
        except FileNotFoundError:
            return "not found"

        output = (result.stdout or "") + (result.stderr or "")
        output = output.strip()
        if not output or not self.version_regex:
            return "unknown"

        match = re.search(self.version_regex, output)
        return match.group(1) if match else "unknown"

    def get_binary_location(self):
        location = shutil.which(self.name)
        return location if location else "not found"


class SystemLibrary:
    def __init__(self, name, min_version, version_regex=None):
        self.name = name
        self.min_version = min_version
        self.version_regex = version_regex or r"^Version:\s*(\S+)"

    def get_installed_version(self):
        try:
            result = subprocess.run(
                ["apt", "show", self.name],
                check=False,
                text=True,
                capture_output=True,
            )
        except FileNotFoundError:
            return "not found"

        output = (result.stdout or "") + (result.stderr or "")
        output = output.strip()
        if not output:
            return "unknown"

        match = re.search(self.version_regex, output, flags=re.MULTILINE)
        return match.group(1) if match else "unknown"

    def get_binary_location(self):
        return "-"


class ToolList:
    def __init__(self):
        self.tools = []

    def add_tool(self, name, min_version, version_regex=None):
        tool = CLITool(name, min_version, version_regex)
        self.tools.append(tool)

    def add_system_library(self, name, min_version, version_regex=None):
        library = SystemLibrary(name, min_version, version_regex)
        self.tools.append(library)

    def display(self):
        table_data = [
            [
                tool.name,
                tool.min_version,
                tool.get_installed_version(),
                tool.get_binary_location(),
            ]
            for tool in self.tools
        ]
        print(
            tabulate(
                table_data,
                headers=[
                    "Tool",
                    "Minimum Version",
                    "Installed Version",
                    "Binary Location",
                ],
                tablefmt="grid",
                disable_numparse=[1, 2],
            )
        )


def check_env():
    result = subprocess.run(
        'apt list --installed | grep "libstd"',
        shell=True,
        check=False,
        text=True,
        capture_output=True,
    )
    print(result.stdout or "")
    if result.stderr:
        print(result.stderr)

    build_tools = ToolList()
    build_tools.add_tool("make", "4.0", r"GNU Make\s+([0-9.]+)")
    build_tools.add_tool("cmake", "3.15", r"cmake\s+version\s+([0-9.]+)")
    build_tools.add_tool("ninja", "1.10", r"([0-9.]+)")
    build_tools.add_tool("clang-18", "18.0", r"clang version\s+([0-9.]+)")
    build_tools.add_system_library("libsystemd-dev", "233")
    build_tools.add_system_library("build-essential", "12.4")
    print("Tools Required for Building:")
    build_tools.display()


if __name__ == "__main__":
    check_env()
