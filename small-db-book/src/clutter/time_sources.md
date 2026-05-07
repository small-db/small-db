# Time Sources

A database is a time machine: every write gets a timestamp, every read picks a snapshot, every distributed protocol bets on bounded clock skew. The numbers it stamps onto rows ultimately come from a kernel clocksource, and the kernel clocksource ultimately comes from a piece of silicon (or a packet from another machine). This post walks the menu of time sources used in production — first the per-machine ones a Linux kernel will choose between, then the network-based ones that synchronize machines, then what each major cloud provider actually wires up underneath their VMs.

## Why It Matters

Three dependencies that the rest of small-db (and any MVCC database) are built on:

- **Monotonic, fast `clock_gettime`.** Every commit calls it to stamp `commit_ts`; every read calls it to pick `snapshot_ts`. A 25 ns call vs. a 1 µs call is two orders of magnitude in the hot path. A non-monotonic source — one that jumps backwards under any condition — silently breaks `version_ts` ordering.
- **Bounded skew across nodes.** [Closed Timestamps](../distributed_database/closed_timestamps.md), HLC, and Spanner-style TrueTime all assume the difference between two nodes' clocks is bounded by a known ε. ε comes from the network time protocol, not from the local clocksource. NTP gives ε ≈ tens of milliseconds; PTP gives sub-microsecond; atomic clocks give ε ≈ 1 ms regardless of network.
- **Cross-VM consistency.** A migrated VM, a paused VM, a host with frequency scaling — any of these can perturb a guest's time. The clocksource the guest selects has to survive these.

The kernel exposes its menu at `/sys/devices/system/clocksource/clocksource0/`:

```
$ cat /sys/devices/system/clocksource/clocksource0/available_clocksource
tsc hpet acpi_pm
$ cat /sys/devices/system/clocksource/clocksource0/current_clocksource
tsc
```

Each source has a kernel-assigned *rating* (0–500); the highest-rated stable source wins. `clock_gettime` (CLOCK_REALTIME, CLOCK_MONOTONIC) reads from this layer through the vDSO when possible — i.e., entirely in user space, no syscall.

## Per-Machine Hardware Sources

### TSC — Time Stamp Counter

A 64-bit free-running counter, one per CPU core, incremented at the CPU's reference frequency. Read with the `RDTSC` instruction (or `RDTSCP` for a serializing variant). Default rating: 300.

- **Latency.** ~5–25 ns. The fastest source available — it's a single instruction.
- **Resolution.** Sub-nanosecond (counts at GHz rate).
- **Caveats.** Pre-Nehalem (2008), TSC drifted with CPU frequency scaling and stopped on idle — useless. Modern x86 has the **invariant TSC** feature (CPUID 80000007h.EDX[8]): the counter ticks at a constant rate independent of P-states and C-states, and the kernel synchronizes the per-core counters at boot. Without invariant TSC, the kernel demotes TSC to "unstable" and falls back to HPET or ACPI PM.
- **Virtualization.** A naive guest TSC is broken: VM live-migration moves the guest to a host with a different TSC value, frequency, or offset. Hypervisors solve this either by trapping `RDTSC` (very slow) or — the modern path — by exposing a *virtualized* invariant TSC (Intel VMX TSC scaling + offset). Most current cloud providers expose invariant TSC to guests, which is why TSC is often the selected clocksource even in VMs.

TSC is what you want. Verify it's selected and stays selected.

### KVM-clock

A paravirtualized clocksource specific to KVM guests. The host writes a per-vCPU page with `(tsc_timestamp, system_time, tsc_to_system_mul, tsc_shift)`; the guest reads the page and computes `system_time + (rdtsc - tsc_timestamp) * mul >> shift`. Default rating: 400 (preferred over native TSC inside a KVM guest).

- **Latency.** ~30–80 ns. Slightly worse than bare-metal TSC because of the page read and arithmetic, but no vmexit on the fast path.
- **Why it exists.** Pre-invariant-TSC virtualization left guests with no good clocksource. KVM-clock gave guests a stable, monotonic, migration-safe clock by pinning it to host time. The host updates the page across migrations, so the guest's `clock_gettime` keeps advancing correctly.
- **When it loses.** On modern CPUs with virtualized invariant TSC, KVM-clock's overhead is pure tax — TSC alone is faster and just as stable. AWS Nitro guests, for instance, prefer TSC.
- **Other paravirt clocks.** Xen has `xen` clocksource, Hyper-V has `hyperv_clocksource_tsc_page`. Same pattern: shared page from host, guest computes locally.

### HPET — High Precision Event Timer

A chipset-level timer (Intel ICH / AMD FCH), memory-mapped, with a frequency programmed at boot (typically 14.318 MHz or 24 MHz). Default rating: 250.

- **Latency.** ~500 ns – 1 µs. The read goes through the chipset and a memory-mapped I/O cycle, which is slow compared to a CPU register.
- **Resolution.** ~70 ns at 14 MHz.
- **Role today.** A reliable fallback when TSC is unstable. Also serves as one of the kernel's interrupt-generating tick sources. On invariant-TSC systems, HPET sits in `available_clocksource` but rarely gets picked.
- **Virtualization.** Most hypervisors emulate HPET via vmexit on every read — slower still in a guest. Cloud guests should not be on HPET if they can avoid it.

### ACPI PM — ACPI Power Management Timer

A 24-bit (or 32-bit) counter at 3.579545 MHz (a holdover from NTSC color-burst frequency), defined by the ACPI specification. Read via port I/O. Default rating: 200.

- **Latency.** ~600–1000 ns. Port I/O is even slower than HPET's MMIO.
- **Resolution.** ~280 ns at 3.58 MHz.
- **Role today.** The last-resort hardware source. The kernel falls to it when both TSC and HPET are unusable (broken silicon, exotic platform, hypervisor without good emulation). 24-bit counters wrap every ~4.7 seconds; the kernel handles wrap detection, but the bookkeeping is not free.

### PIT — Programmable Interval Timer

The original IBM PC/AT 8254 timer, 1.193182 MHz. Effectively obsolete; the kernel keeps it around for boot bring-up and IRQ generation on antique systems. Latency in the µs–ms range. Never selected as a primary clocksource on modern hardware.

### Refined jiffies / jiffies

A pure software counter incremented by a periodic interrupt (HZ = 100/250/300/1000 depending on kernel config). Resolution: 1–10 ms. Used as a fallback when no hardware source is registered, and as a coarse tick for things that don't need precision (e.g. `schedule()` accounting).

### ART — Always Running Timer

Intel-only (Skylake+), exposed via the Time Coordinated Computing extension. ART is a fixed-rate counter shared between the CPU and devices like NICs and GPUs, allowing hardware-level cross-device timestamp correlation. The kernel pairs ART with TSC for converting device timestamps into system time. Not directly user-visible as a clocksource, but underlies hardware PTP timestamping on modern Intel platforms.

## Network Time Sources

The local clocksource gives you fast, monotonic, per-machine time. To make two machines agree on time, you need to sync. Two protocols and two reference physical sources dominate:

### NTP — Network Time Protocol

Software: `ntpd`, `chrony` (modern default on RHEL/CentOS/Ubuntu Server). Servers exchange UDP packets, estimate round-trip and offset, and slew/step the local clock. Public NTP pools (e.g. `pool.ntp.org`) reach ~10–50 ms accuracy over the open internet. A LAN NTP server reaches <1 ms. Cloud-provider NTP services (next section) target tens to hundreds of µs.

NTP works on commodity hardware with zero special equipment, which is why it's universal. Its accuracy ceiling is the variance of network round-trip — on a noisy WAN, it can't get tighter than a few ms.

### PTP — Precision Time Protocol (IEEE 1588)

Designed for sub-µs accuracy on a LAN. Two requirements separate it from NTP:

1. **Hardware timestamping.** PTP packets get timestamped on the NIC the instant they hit the wire, bypassing the kernel/userspace stack. The NIC exposes a *PTP Hardware Clock* (PHC) — `/dev/ptp0` — that's separate from the system clock. `phc2sys` is the daemon that disciplines the system clock to the PHC.
2. **Boundary/transparent clocks.** Switches between sender and receiver are PTP-aware and correct for queueing delay.

With both: ~100 ns – 1 µs accuracy across a datacenter. Without hardware timestamping, PTP degrades to NTP-class precision.

### GPS / PPS

A direct physical reference. A GPS receiver outputs a 1 Hz PPS (pulse-per-second) signal accurate to ~10–100 ns relative to UTC. Connected via serial DCD or a dedicated GPSDO card; `gpsd` + `chrony`/`ntpd` discipline the system clock. The reference for any datacenter that runs its own time fabric.

### Atomic clocks

Rubidium oscillators (~10⁻¹¹ stability, hours of holdover) and cesium primary standards (~10⁻¹³, days). What backs Spanner's TrueTime — each Google datacenter has multiple time-master servers, each with an atomic clock and a GPS receiver, and the masters cross-check each other. The atomic clock provides holdover when GPS goes out; GPS provides absolute calibration when the atomic clock drifts.

## Cloud Providers

Cloud providers run NTP/PTP infrastructure for their tenants, with a regional GPS+atomic backbone underneath. The headline numbers and access methods:

### AWS Time Sync Service

- **NTP since 2017.** Reachable from any EC2 instance at `169.254.169.123` (link-local); the Nitro hypervisor proxies to a regional NTP fleet backed by GPS + atomic. Typical accuracy: ~hundreds of µs.
- **PTP since 2023.** Available on Nitro instances of c7g/r7g/m7g and newer; the NIC exposes a PHC (`/dev/ptp0`) and the documented target is ~µs. Used by AWS-internal services like RDS/Aurora multi-writer replication.
- **Guest clocksource.** Modern Nitro instances expose a virtualized invariant TSC; the default selected source is `tsc`, not `kvm-clock`.

### Google Cloud / Spanner TrueTime

- **NTP for general VMs.** `metadata.google.internal` provides NTP backed by Google's regional time infrastructure (GPS + atomic).
- **TrueTime for Spanner.** Internal-only API: `TT.now()` returns an interval `[earliest, latest]` with a bounded uncertainty ε that's typically 1–7 ms. Spanner's commit-wait protocol blocks `commit_ts` until the local clock has provably passed it, which is what lets Spanner serve external-consistency reads without reader-side coordination.
- **Time masters.** Each datacenter has multiple time masters: Marzullo's algorithm fuses GPS + atomic clock readings; client machines poll several masters and apply Marzullo to the responses.

### Azure

- **Host Time Sync.** Hyper-V exposes a synthetic time-sync device over VMBus; the guest integration service (`hyperv_clocksource_tsc_page` for clocksource, ICTimeSync for sync) keeps guest time aligned with the host.
- **Azure NTP.** `time.windows.com` for general use; per-region NTP endpoints for tighter accuracy. Sub-millisecond on the same network.
- **Precision Time Protocol.** Available on certain SKUs in 2024+; targets ~µs.

### Alibaba Cloud / Tencent Cloud

- **NTP service** at `ntp.aliyun.com` / `time1.cloud.tencent.com`, GPS+atomic-backed, sub-ms typical.
- **PTP** available on selected ECS instance families with hardware timestamping.

### Bare-metal / on-prem

GPS+PPS receiver feeding a stratum-1 chrony/ntpd, optional atomic clock for holdover, PTP across the LAN if sub-ms is needed. The reference implementations are Facebook's Time Card (open-hardware GPS+atomic+PTP card) and the Open Compute Time Appliance Project.

## Comparison

### Per-machine clocksources

| Source | Typical access | Resolution | Monotonic | VM-safe | Default rating | Selected when |
|---|---|---|---|---|---|---|
| **TSC** (invariant) | 5–25 ns | sub-ns | yes | yes (with VMX scaling) | 300 | bare metal & modern guests |
| **KVM-clock** | 30–80 ns | ~ns | yes | yes (paravirt) | 400 | KVM guests on legacy hosts |
| **HPET** | 500 ns – 1 µs | ~70 ns | yes | yes (slow under emulation) | 250 | TSC unstable |
| **ACPI PM** | 600 ns – 1 µs | ~280 ns | yes (with wrap handling) | yes | 200 | TSC + HPET unavailable |
| **PIT** | µs – ms | ~840 ns | yes | yes | 110 | boot-time bring-up only |
| **refined_jiffies** | <1 ns (RAM read) | 1–10 ms | yes | yes | 2 | no hardware source registered |
| **Hyper-V TSC page** | ~50 ns | ~ns | yes | yes (paravirt) | 400 | Hyper-V guests |
| **Xen** | ~50 ns | ~ns | yes | yes (paravirt) | 400 | Xen guests |

### Network sync mechanisms

| Mechanism | Accuracy | Hardware needed | Cross-WAN | Holdover | Used by |
|---|---|---|---|---|---|
| **Public NTP** (`pool.ntp.org`) | 10–50 ms | none | yes | local clock drift | hobbyists, default Linux installs |
| **Cloud NTP** (AWS/GCP/Azure) | 100 µs – 1 ms | none (link-local) | within region | provider-side | typical cloud workloads |
| **PTP, software** | 10 µs – 100 µs | none | LAN only | drift | low-end PTP deployments |
| **PTP, hardware** | 100 ns – 1 µs | PHC NIC + PTP switches | LAN only | drift | financial trading, AWS Nitro PTP, Spanner replicas |
| **GPS / PPS** | 10–100 ns | GPS receiver + antenna | n/a (direct) | seconds | datacenter stratum-1 |
| **Atomic clock (Rb)** | ~10⁻¹¹ | rubidium oscillator | n/a | hours | Spanner time masters |
| **Atomic clock (Cs)** | ~10⁻¹³ | cesium standard | n/a | days | national time labs, defense |

### Cloud provider time fabrics at a glance

| Provider | Default in-VM clocksource | Tenant-facing NTP | Tenant-facing PTP | Internal reference |
|---|---|---|---|---|
| **AWS** | TSC (Nitro) | `169.254.169.123`, ~hundreds of µs | yes, c7g/r7g/m7g+, ~µs | regional GPS + atomic |
| **GCP** | TSC | `metadata.google.internal` | not tenant-facing | TrueTime: GPS + atomic per DC; Marzullo fusion |
| **Azure** | Hyper-V TSC page / TSC | `time.windows.com` + regional | yes, selected SKUs (2024+) | regional GPS + atomic |
| **Alibaba** | TSC / KVM-clock | `ntp.aliyun.com` | selected ECS families | regional GPS + atomic |

## Reading the Tables

- **Order-of-magnitude latency band that matters:** TSC (~10 ns) is two orders faster than HPET/ACPI PM (~1 µs). For a database that calls `clock_gettime` once per row scanned, that gap is the difference between "free" and "shows up in the profile."
- **Default rating ≠ best.** KVM-clock (400) outranks TSC (300) historically because pre-invariant-TSC virtualization broke TSC. On modern Nitro/GCE, the host exposes virtualized invariant TSC and the guest selects TSC over KVM-clock — by deliberate hypervisor configuration, not by the rating.
- **NTP and PTP are not substitutes for the local clocksource; they discipline it.** A node still reads time from TSC at every `clock_gettime`. NTP/PTP only adjust the *offset and rate* applied on top of TSC. That's why a node with a great PTP-disciplined clock can still bog down in TSC-read overhead if the database is timestamp-heavy.
- **The accuracy ceiling for a distributed database without atomic clocks is PTP-class — sub-µs.** That's good enough for HLC and closed-timestamps, not for TrueTime-style commit-wait. Spanner's 1–7 ms ε is *not* limited by atomic clocks (those would give µs); it's limited by the Marzullo-fusion safety margin Google chose to budget.

## Implications for small-db

`small::txn::Txn` calls `clock_gettime(CLOCK_REALTIME)` to derive `start_ts` and `commit_ts`; the Closed Timestamps mechanism assumes per-node monotonic time. Two checks for any deployment:

```bash
# 1. Confirm the selected source is TSC (or kvm-clock on legacy hypervisors).
cat /sys/devices/system/clocksource/clocksource0/current_clocksource

# 2. Confirm the kernel sees invariant TSC.
grep -m1 -E '^flags' /proc/cpuinfo | tr ' ' '\n' | grep -E '^(constant_tsc|nonstop_tsc|tsc_reliable)$'
```

For Jepsen runs (3 VirtualBox VMs on one host), all three guests share one host TSC; cross-node skew is bounded by hypervisor scheduling jitter, not by NTP. That's why the Jepsen tests can ignore NTP sync and still observe meaningful closed-timestamp behavior. A real multi-host deployment would need at least chrony on every node and ideally PTP if the database's ε budget were tight enough to matter.

## References

- Linux kernel source: `kernel/time/clocksource.c` and the platform clocksource drivers under `drivers/clocksource/`.
- Intel SDM Vol. 3B §17.17 (Time-Stamp Counter).
- ACPI Specification §4.8 (Power Management Timer).
- IEEE 1588-2019 (PTP).
- *Spanner: Google's Globally-Distributed Database*, Corbett et al., OSDI 2012 — TrueTime architecture.
- AWS docs: *Set the time for your Amazon EC2 instance* and *Amazon Time Sync Service Precision Hardware Clock*.
- Facebook Time Card: <https://github.com/opencomputeproject/Time-Appliance-Project>.
