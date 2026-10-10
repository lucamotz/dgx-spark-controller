# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""``python -m controller`` runs the local loopback controller service."""

from controller.main import main

if __name__ == "__main__":
    raise SystemExit(main())
