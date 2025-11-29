#!/usr/bin/env python


import cxc_toolkit


def main():
    # disable the KVM kernel extension
    cxc_toolkit.exec.run_command("sudo modprobe -r kvm_amd", start_new_session=False)

    cxc_toolkit.exec.run_command("vagrant up", work_dir="small-db-jepsen/vagrant")

    # update dns resolution
    cxc_toolkit.exec.run_command(
        "sudo $(go env GOPATH)/bin/hostctl add small-db-jepsen --from ./nodes",
        work_dir="small-db-jepsen/vagrant",
        start_new_session=False,
    )

    output, _ = cxc_toolkit.exec.run_command(
        "vagrant status", work_dir="small-db-jepsen/vagrant", stream_output=False
    )

    nodes = []
    statuses = []
    for line in output.split("\n"):
        if "(virtualbox)" in line:
            nodes.append(line.split()[0])
            statuses.append(line.split()[1])

    # assert all nodes are running
    assert all(status == "running" for status in statuses)

    # # debug: only use asia node
    # nodes = [nodes[0]]

    node_args = " ".join(f"--node {node}" for node in nodes)

    # run jepsen test
    #
    # use private key since upload-with-password is broken on jepsen 0.3.9:
    # https://github.com/jepsen-io/jepsen/blob/a9763068b168738d31a2388bd4d9dc79d7bc9730/jepsen/src/jepsen/control/scp.clj#L59-L71
    cxc_toolkit.exec.run_command(
        f"lein run test {node_args} --ssh-private-key ~/.vagrant.d/insecure_private_key --username vagrant",
        work_dir="small-db-jepsen",
    )


if __name__ == "__main__":
    main()
