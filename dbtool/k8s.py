"""kubectl / kubectx / kubens integration for transferring dumps to/from pods."""

import shutil
import subprocess
from pathlib import Path

from .ui import console


def _run(cmd: list[str], check=True, capture=True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=capture, text=True, check=check, timeout=30)


def _run_long(cmd: list[str]) -> subprocess.CompletedProcess:
    """for kubectl cp which can take a while — stream output."""
    return subprocess.run(cmd, text=True, timeout=600)


# ── tool checks ──────────────────────────────────────────────────────────────

def check_tools() -> dict[str, bool]:
    """check which k8s tools are available."""
    tools = {}
    for name in ("kubectl", "kubectx", "kubens"):
        tools[name] = shutil.which(name) is not None
    return tools


def require_tools() -> bool:
    """check tools and print warnings. returns True if usable."""
    tools = check_tools()
    if not tools["kubectl"]:
        console.print("[error]kubectl not found — install it first[/]")
        return False
    ok = True
    if not tools["kubectx"]:
        console.print("[warning]kubectx not found — install via: kubectl krew install ctx[/]")
        console.print("[dim]  falling back to: kubectl config get-contexts[/]")
        ok = True  # still usable
    if not tools["kubens"]:
        console.print("[warning]kubens not found — install via: kubectl krew install ns[/]")
        console.print("[dim]  falling back to: kubectl get namespaces[/]")
        ok = True
    return ok


# ── context ──────────────────────────────────────────────────────────────────

def get_current_context() -> str | None:
    try:
        r = _run(["kubectl", "config", "current-context"])
        return r.stdout.strip() or None
    except Exception:
        return None


def list_contexts() -> list[tuple[str, bool]]:
    """returns [(name, is_current)] using kubectx or fallback."""
    tools = check_tools()
    if tools["kubectx"]:
        try:
            r = _run(["kubectx"])
            current = get_current_context()
            return [(c.strip(), c.strip() == current) for c in r.stdout.splitlines() if c.strip()]
        except Exception:
            pass
    # fallback
    try:
        r = _run(["kubectl", "config", "get-contexts", "-o", "name"])
        current = get_current_context()
        return [(c.strip(), c.strip() == current) for c in r.stdout.splitlines() if c.strip()]
    except Exception:
        return []


def switch_context(name: str) -> bool:
    tools = check_tools()
    try:
        if tools["kubectx"]:
            _run(["kubectx", name])
        else:
            _run(["kubectl", "config", "use-context", name])
        return True
    except Exception as e:
        console.print(f"[error]failed to switch context: {e}[/]")
        return False


# ── namespace ────────────────────────────────────────────────────────────────

def get_current_namespace() -> str:
    tools = check_tools()
    if tools["kubens"]:
        try:
            r = _run(["kubens", "-c"])
            ns = r.stdout.strip()
            if ns:
                return ns
        except Exception:
            pass
    # fallback
    try:
        r = _run(["kubectl", "config", "view", "--minify",
                   "-o", "jsonpath={..namespace}"])
        return r.stdout.strip() or "default"
    except Exception:
        return "default"


def list_namespaces() -> list[str]:
    tools = check_tools()
    if tools["kubens"]:
        try:
            r = _run(["kubens"])
            return [n.strip() for n in r.stdout.splitlines() if n.strip()]
        except Exception:
            pass
    try:
        r = _run(["kubectl", "get", "namespaces", "-o",
                   "jsonpath={.items[*].metadata.name}"])
        return r.stdout.strip().split()
    except Exception:
        return []


def switch_namespace(name: str) -> bool:
    tools = check_tools()
    try:
        if tools["kubens"]:
            _run(["kubens", name])
        else:
            _run(["kubectl", "config", "set-context", "--current",
                   f"--namespace={name}"])
        return True
    except Exception as e:
        console.print(f"[error]failed to switch namespace: {e}[/]")
        return False


# ── pods ─────────────────────────────────────────────────────────────────────

def list_pods(namespace: str | None = None) -> list[dict]:
    """returns list of {name, status, ready, age} dicts."""
    cmd = ["kubectl", "get", "pods", "-o",
           "jsonpath={range .items[*]}{.metadata.name}|{.status.phase}|{.status.containerStatuses[0].ready}\\n{end}"]
    if namespace:
        cmd.extend(["-n", namespace])
    try:
        r = _run(cmd)
        pods = []
        for line in r.stdout.strip().splitlines():
            parts = line.strip().split("|")
            if len(parts) >= 3:
                pods.append({
                    "name": parts[0],
                    "status": parts[1],
                    "ready": parts[2] == "true",
                })
        return pods
    except Exception:
        return []


# ── kubectl cp ───────────────────────────────────────────────────────────────

def kube_cp_to_pod(
    local_path: Path,
    pod_name: str,
    remote_path: str,
    namespace: str | None = None,
    container: str | None = None,
) -> bool:
    """copy local file/dir to pod. returns True on success."""
    remote = f"{pod_name}:{remote_path}"
    cmd = ["kubectl", "cp", str(local_path), remote]
    if namespace:
        cmd.extend(["-n", namespace])
    if container:
        cmd.extend(["-c", container])

    console.print(f"[dim]$ {' '.join(cmd)}[/]")
    try:
        _run_long(cmd)
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[error]kubectl cp failed: {e}[/]")
        return False
    except subprocess.TimeoutExpired:
        console.print("[error]kubectl cp timed out[/]")
        return False


def kube_cp_from_pod(
    pod_name: str,
    remote_path: str,
    local_path: Path,
    namespace: str | None = None,
    container: str | None = None,
) -> bool:
    """copy file/dir from pod to local. returns True on success."""
    remote = f"{pod_name}:{remote_path}"
    cmd = ["kubectl", "cp", remote, str(local_path)]
    if namespace:
        cmd.extend(["-n", namespace])
    if container:
        cmd.extend(["-c", container])

    console.print(f"[dim]$ {' '.join(cmd)}[/]")
    try:
        _run_long(cmd)
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[error]kubectl cp failed: {e}[/]")
        return False
    except subprocess.TimeoutExpired:
        console.print("[error]kubectl cp timed out[/]")
        return False