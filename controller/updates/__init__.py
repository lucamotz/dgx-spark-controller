# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Scheduled system updates of the configured nodes.

The controller downloads every update itself (from the Internet or a local
directory) and serves it to the nodes through its update relay, installs it
with a root-owned helper on each node, reboots the nodes, and restores the
deployments that were serving before.
"""
