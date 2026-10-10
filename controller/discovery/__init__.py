# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Read-only LAN discovery primitives for the onboarding wizard.

The modules in this package find candidate DGX Spark nodes on the operator's own
private network and prepare an SSH connection the operator explicitly approves.
They never mutate a node, never send a password, never read private key contents,
and never probe anything outside private (RFC 1918, CGNAT, or link-local) IPv4
ranges. Probes are bounded in host count, concurrency, and time.

Submodules: ``network`` (local interfaces and scan-target validation), ``mdns``
(best-effort multicast-DNS browse), ``scanner`` (bounded TCP/HTTP probing and
classification), ``hostkeys`` (host-key scanning and the controller's pinned
``known_hosts`` store), ``sshkeys`` (finding or generating a client key pair),
and ``commands`` (the one bounded, argv-only way to run a local tool).
"""
