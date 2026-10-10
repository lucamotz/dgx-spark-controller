# Install and run

DGX Spark Controller has two entry points that share one codebase on Linux and macOS:

- `dgx-spark-controller` runs the local service: a loopback-only HTTP API and the web UI. It uses only the Python standard library.
- `dgx-spark-controller-app` is the desktop launcher. It starts the local service (or reuses one that is already running) and shows the UI in a native window, or in your browser when no window backend is available.

Closing the app never unloads a remote model. See [What closing the app does](#what-closing-the-app-does).

## Requirements

- Python 3.12 or newer and an OpenSSH client.
- macOS 13 or newer, or Linux. A Linux machine without a desktop works too; you then open the UI through an SSH port forward.
- [uv](https://docs.astral.sh/uv/) is recommended but optional.

## Run from a checkout

```bash
git clone https://github.com/lucamotz/dgx-spark-controller.git
cd dgx-spark-controller
uv venv --python 3.12 .venv            # or: python3 -m venv .venv
uv pip install --python .venv/bin/python -e .   # or: .venv/bin/python -m pip install -e .
.venv/bin/dgx-spark-controller-app
```

Without the optional window extra the launcher opens your default browser and keeps running in the terminal; press Ctrl-C to stop the local service. `python -m controller.launcher` is equivalent to `dgx-spark-controller-app`, and `python -m controller` runs the service alone.

| Launcher option | Effect |
| --- | --- |
| `--port N` | Loopback port of the local service (default `18876`). |
| `--browser` | Use the system browser even if a native window is available. |
| `--no-open` | Run the local service in the foreground without opening any UI. |
| `--python PATH` | Interpreter for the service child (default: the launcher's own). |

If a healthy controller already answers on the port, the launcher does not start a second one. It only opens the UI, and closing the window leaves that service running.

### Where files live

| | macOS | Linux |
| --- | --- | --- |
| Configuration | `~/Library/Application Support/DGXSparkController/config` | `$XDG_CONFIG_HOME/dgx-spark-controller` (`~/.config/...`) |
| Data and journal | `~/Library/Application Support/DGXSparkController/data` | `$XDG_DATA_HOME/dgx-spark-controller` (`~/.local/share/...`) |
| Service log (`controller-app.log`) | `~/Library/Logs/DGXSparkController` | `$XDG_STATE_HOME/dgx-spark-controller/logs` (`~/.local/state/...`) |

Set `DGX_CONTROLLER_HOME=/some/dir` to keep everything below `/some/dir/{config,data,logs}` instead, for example for a test setup. The controller creates these directories with mode `0700`.

## macOS

### Native window

The `desktop` extra installs [pywebview](https://pywebview.flowrl.com/), which uses the system WebKit through PyObjC:

```bash
uv pip install --python .venv/bin/python -e '.[desktop]'
.venv/bin/dgx-spark-controller-app
```

### App bundle for Finder, Dock and Spotlight

```bash
packaging/macos/build_app.sh                  # writes build/DGXSparkController.app
ditto build/DGXSparkController.app ~/Applications/DGXSparkController.app
```

The bundle is a thin launcher. It runs `<checkout>/.venv/bin/python -m controller.launcher` from the checkout it was built from, so pulling new code needs no rebuild. Rebuild it after moving the checkout. The script generates the icon with `sips` and `iconutil` and signs the bundle ad hoc when `codesign` is available. Run it again at any time; it replaces the previous bundle. Without pywebview in `.venv` the app falls back to the browser and the build prints a warning.

To keep reviewed profiles, node configuration and the journal in a separate
directory, for example a private repository next to this public checkout, bake
it into the bundle as `DGX_CONTROLLER_HOME`:

```bash
packaging/macos/build_app.sh --home ~/my-cluster   # uses ~/my-cluster/{config,data,logs}
```

The code still comes from this checkout, so the private directory holds only
`config/nodes.json`, `config/profiles/*.json` and machine-local state. A
`DGX_CONTROLLER_HOME` set in the environment takes precedence.

Quitting the app (Cmd-Q, closing the window) stops only the local service it started.

To uninstall, delete the app bundle and `.venv`. The configuration, journal and logs listed above are kept until you delete them.

## Linux

### Desktop install

```bash
packaging/linux/install.sh                  # browser UI
packaging/linux/install.sh --with-window    # native GTK window
```

The installer works per user and never uses `sudo`. It creates `<checkout>/.venv` when missing (with `uv venv --python 3.12` if uv is available, otherwise `python3 -m venv`), installs the checkout in editable mode, and adds:

- `$XDG_DATA_HOME/applications/dgx-spark-controller.desktop` (default `~/.local/share/applications`), which starts `.venv/bin/dgx-spark-controller-app`
- `$XDG_DATA_HOME/icons/hicolor/256x256/apps/dgx-spark-controller.png`

`--with-window` installs the `desktop-gtk` extra. PyGObject is built from source and needs system packages, which the installer prints but does not install:

```bash
# Debian / Ubuntu 24.04 (on 22.04 use libgirepository1.0-dev)
sudo apt install libgirepository-2.0-dev libcairo2-dev pkg-config python3-dev gir1.2-webkit2-4.1
# Fedora
sudo dnf install gobject-introspection-devel cairo-gobject-devel pkgconf-pkg-config python3-devel webkit2gtk4.1
```

A Qt window is the alternative: `uv pip install --python .venv/bin/python -e '.[desktop-qt]'`.

The launcher uses a native window only when `DISPLAY` or `WAYLAND_DISPLAY` is set and GTK or Qt bindings are importable; otherwise it states why and uses the browser. Without any display it only prints the URL.

### Headless machine

Run the service on the Linux host and open the UI from your workstation through an SSH tunnel:

```bash
# on the Linux host
.venv/bin/dgx-spark-controller-app --no-open
# on your workstation, then browse to http://127.0.0.1:18876/
ssh -N -L 18876:127.0.0.1:18876 user@linux-host
```

The launcher and the user unit bind the service to `127.0.0.1` only; do not expose it on other interfaces.

### systemd user service

```bash
packaging/linux/install.sh --service
systemctl --user enable --now dgx-spark-controller.service
journalctl --user -u dgx-spark-controller.service -f
```

`--service` writes `$XDG_CONFIG_HOME/systemd/user/dgx-spark-controller.service`, which runs `.venv/bin/dgx-spark-controller --host 127.0.0.1 --port 18876` with `Restart=on-failure` and basic hardening (`NoNewPrivileges`, `PrivateTmp`, `UMask=0077`). The installer reloads systemd but never enables the unit for you. Run `loginctl enable-linger` if the service should keep running while you are logged out. If the unit fails with status `226/NAMESPACE`, your system does not allow user namespaces for user services; delete the `PrivateTmp=yes` line and run `systemctl --user daemon-reload`.

While the service runs, the desktop entry and `dgx-spark-controller-app` reuse it and leave it running when the window closes.

### Uninstall

```bash
packaging/linux/uninstall.sh                 # desktop entry, icon, user unit
packaging/linux/uninstall.sh --remove-venv   # also .venv, if install.sh created it
```

The uninstaller disables and removes only the files that `install.sh` wrote. It keeps the controller's configuration, journal and logs.

## What closing the app does

- The launcher stops only the local service process that it started: SIGTERM, then SIGKILL after 10 seconds. Closing the window, Cmd-Q, Ctrl-C, SIGTERM and SIGHUP all do this.
- It never calls a deployment or unload API. Remote models keep serving until an operator explicitly unloads them in the UI.
- A service it did not start (the systemd unit, or another window's service) keeps running.
- If the launcher is killed outright, the service notices that its parent is gone and shuts itself down. Remote models are not affected.

## Troubleshooting

- **"Port 18876 ... is used by a program that is not a healthy DGX Spark Controller"**: another program owns the port. Stop it or pass `--port`.
- **"The local controller exited with code N"**: the message includes the last lines of `controller-app.log` and its full path; a missing or invalid configuration is the usual cause.
- **"Native window unavailable: ..."**: the reason names the missing piece (pywebview, GTK or Qt bindings, or a display). The browser UI works the same.
