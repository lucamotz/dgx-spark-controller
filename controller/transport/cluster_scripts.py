# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Fixed remote scripts that start and stop the Docker ranks of a cluster recipe.

The recipe's own start and stop scripts stay the source of truth for launch
flags; each is re-verified on the node (readable, Bash syntax, reviewed
SHA-256) immediately before it runs. The controller itself only renders a
private environment overlay beside the operator ``.env`` and one ownership
marker per rank.
"""

from __future__ import annotations

import shlex

from controller.transport.managed_config import ClusterScriptRecipe

# Shell expressions of the ownership-marker directories; the observation probe
# reads exactly what these scripts write.
MARKER_ROOT = "${XDG_STATE_HOME:-$HOME/.local/state}/dgx-controller"
LEGACY_MARKER_ROOT = "${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/dgx-controller"


def environment_overlay_script(recipe: ClusterScriptRecipe, node_id: str) -> str:
    """Copy the operator environment and pin only profile-reviewed values."""
    replacements: list[str] = []
    for key, value in sorted(recipe.environment_for(node_id).items()):
        replacements.extend(
            [
                'next="${tmp}.next"',
                f"awk -F= -v key={shlex.quote(key)} '$1 != key {{ print }}' "
                '"$tmp" > "$next"',
                f"printf '%s=%s\\n' {shlex.quote(key)} {shlex.quote(value)} "
                '>> "$next"',
                'mv "$next" "$tmp"',
            ]
        )
    replacement_script = "\n".join(replacements)
    return f"""set -eu
umask 077
source_path={shlex.quote(recipe.environment_path)}
managed_path={shlex.quote(recipe.managed_environment_path)}
test -r "$source_path"
tmp=$(mktemp "${{managed_path}}.tmp.XXXXXX")
cleanup() {{
  test ! -e "$tmp" || unlink "$tmp"
  test ! -e "${{tmp}}.next" || unlink "${{tmp}}.next"
}}
trap cleanup EXIT HUP INT TERM
cp "$source_path" "$tmp"
{replacement_script}
chmod 600 "$tmp"
mv "$tmp" "$managed_path"
trap - EXIT HUP INT TERM
"""


def write_marker_script(recipe: ClusterScriptRecipe) -> str:
    """Record controller ownership in persistent per-user state.

    The marker lives under ``XDG_STATE_HOME`` so ownership survives the loss
    of the runtime directory on logout or reboot.
    """
    return f"""set -eu
umask 077
root={MARKER_ROOT}
install -d -m 700 "$root"
printf '%s\n' {shlex.quote(recipe.deployment_id)} > "$root/{recipe.marker_name}"
chmod 600 "$root/{recipe.marker_name}"
"""


def remove_marker_script(recipe: ClusterScriptRecipe) -> str:
    """Remove the ownership marker, including one in the legacy runtime dir."""
    return f"""set -u
root={MARKER_ROOT}
test ! -f "$root/{recipe.marker_name}" || unlink "$root/{recipe.marker_name}"
legacy_root={LEGACY_MARKER_ROOT}
test ! -f "$legacy_root/{recipe.marker_name}" \\
  || unlink "$legacy_root/{recipe.marker_name}"
"""


def launch_script(
    recipe: ClusterScriptRecipe, node_id: str, stop_timeout_seconds: float
) -> str:
    """Start the pinned launcher as a durable unit once the overlay is proven."""
    environment_checks = "\n".join(
        (
            f"grep -Fqx -- {shlex.quote(f'{key}={value}')} "
            f"{shlex.quote(recipe.managed_environment_path)}"
        )
        for key, value in sorted(recipe.environment_for(node_id).items())
    )
    prologue = verified_script_prologue(recipe.start_script_path, recipe.start_sha256)
    return f"""{prologue}{environment_checks}
cd {shlex.quote(recipe.working_directory)}
{_launcher_reset_lines(recipe)}systemd-run --user --unit "$unit" \
  --property=RemainAfterExit=yes \
  --property=TimeoutStopSec={max(30, int(stop_timeout_seconds))} \
  --setenv=ENV_FILE={shlex.quote(recipe.managed_environment_path)} \
  /bin/bash "$script_path"
"""


def launcher_reset_script(recipe: ClusterScriptRecipe) -> str:
    """Stop a still-running launcher before the stop script runs."""
    return "set -u\n" + _launcher_reset_lines(recipe)


def stop_script(recipe: ClusterScriptRecipe) -> str:
    """Run the pinned stop script against the rendered (or base) environment."""
    if recipe.per_node:
        # A rank whose start failed before its overlay was rendered still has
        # the installed base environment, which names the exact container.
        environment_lines = (
            f"env_file={shlex.quote(recipe.managed_environment_path)}\n"
            'test -r "$env_file" || '
            f"env_file={shlex.quote(recipe.environment_path)}\n"
            'test -r "$env_file"\n'
            f"cd {shlex.quote(recipe.working_directory)}\n"
            'ENV_FILE="$env_file" /bin/bash "$script_path"\n'
        )
    else:
        environment_lines = (
            f"test -r {shlex.quote(recipe.managed_environment_path)}\n"
            f"cd {shlex.quote(recipe.working_directory)}\n"
            f"ENV_FILE={shlex.quote(recipe.managed_environment_path)} "
            '/bin/bash "$script_path"\n'
        )
    return (
        verified_script_prologue(recipe.stop_script_path, recipe.stop_sha256)
        + environment_lines
    )


def container_logs_script(recipe: ClusterScriptRecipe, line_limit: int) -> str:
    return (
        "set -eu\n"
        f"docker logs --tail {line_limit} "
        f"{shlex.quote(recipe.container_name)} 2>&1 || true\n"
    )


def verified_script_prologue(script_path: str, sha256: str) -> str:
    """Stop the script unless ``script_path`` is readable, valid Bash and pinned.

    The lines set ``script_path`` for the caller's launch line.
    """
    return (
        "set -eu\n"
        f"script_path={shlex.quote(script_path)}\n"
        f"expected_hash={shlex.quote(sha256)}\n"
        'test -r "$script_path"\n'
        'bash -n "$script_path"\n'
        'test "$(sha256sum "$script_path" | awk \'{print $1}\')" = '
        '"$expected_hash"\n'
    )


def _launcher_reset_lines(recipe: ClusterScriptRecipe) -> str:
    return (
        f"unit={shlex.quote(recipe.launcher_unit_name)}\n"
        'systemctl --user stop "$unit" >/dev/null 2>&1 || true\n'
        'systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true\n'
    )
