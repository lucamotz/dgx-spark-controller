# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Require the project SPDX header at the top of every Python file."""

import sys
from pathlib import Path

LICENSE_LINE = "# SPDX-License-Identifier: Apache-2.0"
COPYRIGHT_LINE = (
    "# SPDX-FileCopyrightText: "
    "Copyright contributors to the DGX Spark Controller project"
)
HEADER = f"{LICENSE_LINE}\n{COPYRIGHT_LINE}\n"


def has_header(text: str) -> bool:
    if text.startswith("#!"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
    while text.startswith("# /// script"):
        end = text.find("# ///\n", 1)
        if end == -1:
            return False
        text = text[end + len("# ///\n") :]
    return text.startswith(HEADER)


def add_header(path: Path, text: str) -> None:
    if text.startswith("#!"):
        shebang, _, rest = text.partition("\n")
        path.write_text(f"{shebang}\n{HEADER}\n{rest}", encoding="utf-8")
    else:
        path.write_text(f"{HEADER}\n{text}", encoding="utf-8")


def main(argv: list[str]) -> int:
    fix = "--fix" in argv
    missing = []
    for name in (arg for arg in argv if arg != "--fix"):
        path = Path(name)
        text = path.read_text(encoding="utf-8")
        if not text.strip() or has_header(text):
            continue
        if fix:
            add_header(path, text)
        missing.append(name)
    for name in missing:
        action = "added SPDX header to" if fix else "missing SPDX header:"
        print(f"{action} {name}")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
