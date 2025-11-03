#!/usr/bin/env python3
"""Утилита для оркестровки флота виртуальных машин."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from typing import List

import yaml


@dataclass(frozen=True)
class SSHParams:
    """Параметры подключения по SSH, полученные из конфигурации."""

    key: Path
    user: str
    port: int
    strict_host_key: str

    @classmethod
    def from_config(cls, cfg: Mapping[str, object]) -> "SSHParams":
        ssh_cfg = cfg.get("ssh")
        if not isinstance(ssh_cfg, Mapping):
            raise ValueError("Конфигурация должна содержать раздел 'ssh'")

        try:
            key = Path(os.path.expanduser(str(ssh_cfg["key"]))).resolve()
            user = str(ssh_cfg["user"])
        except KeyError as exc:  # pragma: no cover - защитная проверка
            raise ValueError(f"В конфигурации ssh отсутствует ключ {exc!s}") from exc

        port = int(ssh_cfg.get("port", 22))
        strict_raw = ssh_cfg.get("strict_host_key", "accept-new")
        strict = _normalize_strict_host_key(strict_raw)
        return cls(key=key, user=user, port=port, strict_host_key=strict)

    def _option_args(self) -> List[str]:
        opts = ["-o", f"StrictHostKeyChecking={self.strict_host_key}"]
        if self.strict_host_key in {"no", "accept-new"}:
            opts += ["-o", "UserKnownHostsFile=/dev/null"]
        return opts

    def build_ssh_command(self, host: str, remote_cmd: str | None = None) -> List[str]:
        cmd = [
            "ssh",
            "-i",
            str(self.key),
            "-p",
            str(self.port),
            *self._option_args(),
            f"{self.user}@{host}",
        ]
        if remote_cmd:
            cmd.append(remote_cmd)
        return cmd

    def build_scp_command(self, local_path: Path, host: str, remote_path: str) -> List[str]:
        return [
            "scp",
            "-i",
            str(self.key),
            "-P",
            str(self.port),
            *self._option_args(),
            str(local_path),
            f"{self.user}@{host}:{remote_path}",
        ]

    def rsync_rsh(self) -> str:
        """Строка для передачи в параметр rsync ``-e``."""

        quoted = " ".join(
            shlex.quote(part)
            for part in (
                "ssh",
                "-i",
                str(self.key),
                "-p",
                str(self.port),
                *self._option_args(),
            )
        )
        return quoted


def _normalize_strict_host_key(value: object) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    text = str(value).strip().lower()
    mapping = {
        "false": "no",
        "0": "no",
        "true": "yes",
        "1": "yes",
    }
    normalized = mapping.get(text, text)
    if normalized not in {"yes", "no", "accept-new"}:
        raise ValueError(
            "strict_host_key должен быть одним из: 'yes', 'no', 'accept-new'"
        )
    return normalized


def load_cfg(path: str | Path) -> MutableMapping[str, object]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, MutableMapping):
        raise ValueError("Конфигурационный файл должен содержать словарь")
    return data


def _format_command(cmd: Sequence[str] | str) -> str:
    if isinstance(cmd, str):
        return cmd
    return " ".join(shlex.quote(part) for part in cmd)


def run_local(cmd: Sequence[str] | str, *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    print("[local]", _format_command(cmd))
    kwargs: dict[str, object] = {"check": check, "shell": isinstance(cmd, str)}
    if capture:
        kwargs.update({"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "text": True})
    return subprocess.run(cmd, **kwargs)


def run_ssh(
    host: str,
    remote_cmd: str,
    ssh: SSHParams,
    *,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    print(f"[ssh:{host}]", remote_cmd)
    cmd = ssh.build_ssh_command(host, remote_cmd)
    kwargs: dict[str, object] = {"check": check}
    if capture:
        kwargs.update({"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "text": True})
    return subprocess.run(cmd, **kwargs)


def scp_to(host: str, local_path: Path, remote_path: str, ssh: SSHParams) -> subprocess.CompletedProcess:
    cmd = ssh.build_scp_command(local_path, host, remote_path)
    print(f"[scp:{host}] {local_path} -> {remote_path}")
    return run_local(cmd, check=True)

def _select_fleet(cfg: Mapping[str, object], requested: Iterable[str] | None) -> List[Mapping[str, object]]:
    fleet_cfg = cfg.get("fleet", [])
    if not isinstance(fleet_cfg, list):
        raise ValueError("Раздел 'fleet' в конфигурации должен быть списком хостов")
    if not requested:
        return list(fleet_cfg)

    requested_set = {item.lower() for item in requested}
    matched: set[str] = set()
    selected: List[Mapping[str, object]] = []
    for entry in fleet_cfg:
        if not isinstance(entry, Mapping):
            continue
        aliases = {
            str(entry.get("name", "")).lower(),
            str(entry.get("host", "")).lower(),
        }
        aliases.discard("")
        if aliases & requested_set:
            selected.append(entry)
            matched.update(aliases & requested_set)

    missing = requested_set - matched
    if missing:
        print(
            f"[warn] не найдены хосты: {', '.join(sorted(missing))}",
            file=sys.stderr,
        )
    return selected


def _pool_workers(parallel: int, fleet_size: int) -> int:
    if fleet_size <= 0:
        return 1
    try:
        desired = int(parallel)
    except (TypeError, ValueError):  # pragma: no cover - защитная проверка
        desired = fleet_size
    desired = max(1, desired)
    return min(desired, fleet_size)


def do_run_script(
    cfg: Mapping[str, object],
    script_local: str,
    *,
    image: str | None = None,
    cmd_override: str | None = None,
    hosts: Iterable[str] | None = None,
    parallel: int = 8,
) -> None:
    ssh = SSHParams.from_config(cfg)
    remote_cfg = cfg.get("remote", {})
    if not isinstance(remote_cfg, Mapping):
        raise ValueError("Раздел 'remote' в конфигурации должен быть словарём")

    script_remote = remote_cfg.get("script_path")
    if not script_remote:
        raise ValueError("В конфигурации remote.script_path не задан путь к скрипту")
    script_remote = str(script_remote)

    fleet = _select_fleet(cfg, hosts)
    if not fleet:
        print("[warn] нет подходящих хостов для выполнения команды")
        return

    local_path = Path(script_local).expanduser()
    if not local_path.exists():
        raise FileNotFoundError(f"Локальный скрипт не найден: {local_path}")

    def worker(host_cfg: Mapping[str, object]) -> subprocess.CompletedProcess:
        host_value = host_cfg.get("host")
        if not host_value:
            raise ValueError("Для каждой записи fleet требуется ключ 'host'")
        host = str(host_value)
        scp_to(host, local_path, script_remote, ssh)
        run_ssh(host, f"chmod +x {shlex.quote(script_remote)}", ssh, check=True, capture=False)

        env_parts: List[str] = []
        if image:
            env_parts.append(f"IMAGE={shlex.quote(image)}")
        if cmd_override:
            env_parts.append(f"CMD={shlex.quote(cmd_override)}")

        remote_cmd_parts = [*env_parts, "bash", shlex.quote(script_remote)]
        remote_cmd = " ".join(remote_cmd_parts)
        return run_ssh(host, remote_cmd, ssh, capture=True)

    workers = _pool_workers(parallel, len(fleet))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(worker, host_cfg): host_cfg for host_cfg in fleet}
        for fut in as_completed(futures):
            host_cfg = futures[fut]
            host_value = host_cfg.get("host")
            host = str(host_value) if host_value else "<unknown>"
            try:
                result = fut.result()
                output = (result.stdout or "").strip()
                if output:
                    print(f"\n[{host}] OUTPUT:\n{output}\n")
            except Exception as exc:  # pragma: no cover - параллельный блок трудно покрыть
                print(f"[{host}] ERROR: {exc}", file=sys.stderr)


def do_check(
    cfg: Mapping[str, object],
    *,
    cleanup: bool = False,
    hosts: Iterable[str] | None = None,
    parallel: int = 8,
) -> None:
    ssh = SSHParams.from_config(cfg)
    remote_cfg = cfg.get("remote", {})
    if not isinstance(remote_cfg, Mapping):
        raise ValueError("Раздел 'remote' в конфигурации должен быть словарём")
    folders_raw = remote_cfg.get("folders", [])
    if isinstance(folders_raw, str):
        folders = [folders_raw]
    elif isinstance(folders_raw, Iterable):
        folders = list(folders_raw)
    else:
        raise ValueError("remote.folders должен быть итерируемым списком путей")

    check_cfg = cfg.get("check", {})
    if not isinstance(check_cfg, Mapping):
        check_cfg = {}

    max_age = int(check_cfg.get("max_age_days_tmp", 7))
    list_head = int(check_cfg.get("list_head", 50))

    fleet = _select_fleet(cfg, hosts)
    if not fleet:
        print("[warn] нет подходящих хостов для проверки")
        return

    def worker(host_cfg: Mapping[str, object]) -> subprocess.CompletedProcess:
        host_value = host_cfg.get("host")
        if not host_value:
            raise ValueError("Для каждой записи fleet требуется ключ 'host'")
        host = str(host_value)
        cmds: List[str] = []
        cmds.append(
            "docker ps --format '{{.Names}}  {{.Image}}  {{.Status}}' | grep -E 'neurd-worker|$'"
        )
        cmds.append("df -h | head -n 2")
        for folder in folders:
            folder_str = shlex.quote(str(folder))
            cmds.append(f"echo '--- {folder} ---'; ls -lah {folder_str} | head -n {list_head} || true")
            cmds.append(f"echo 'du -sh {folder}'; du -sh {folder_str} || true")
        if cleanup:
            cmds.append(f"find /data/tmp -type f -mtime +{max_age} -print -delete || true")
        remote_cmd = " && ".join(f"({command})" for command in cmds)
        return run_ssh(host, remote_cmd, ssh, capture=True)

    workers = _pool_workers(parallel, len(fleet))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(worker, host_cfg): host_cfg for host_cfg in fleet}
        for fut in as_completed(futures):
            host_cfg = futures[fut]
            host_value = host_cfg.get("host")
            host = str(host_value) if host_value else "<unknown>"
            name = host_cfg.get("name", host)
            try:
                result = fut.result()
                output = (result.stdout or "").rstrip()
                print(f"\n===== {name} ({host}) =====\n{output}\n")
            except Exception as exc:  # pragma: no cover - параллельный блок трудно покрыть
                print(f"[{host}] ERROR: {exc}", file=sys.stderr)


def do_sync_results(
    cfg: Mapping[str, object],
    *,
    local_dir: str = "results_sync",
    hosts: Iterable[str] | None = None,
    parallel: int = 4,
) -> None:
    ssh = SSHParams.from_config(cfg)
    remote_cfg = cfg.get("remote", {})
    if not isinstance(remote_cfg, Mapping):
        raise ValueError("Раздел 'remote' в конфигурации должен быть словарём")
    results_dir = str(remote_cfg.get("results_dir", "/data/results"))

    fleet = _select_fleet(cfg, hosts)
    if not fleet:
        print("[warn] нет подходящих хостов для синхронизации")
        return

    local_root = Path(local_dir)
    local_root.mkdir(parents=True, exist_ok=True)

    def worker(host_cfg: Mapping[str, object]) -> subprocess.CompletedProcess:
        host_value = host_cfg.get("host")
        if not host_value:
            raise ValueError("Для каждой записи fleet требуется ключ 'host'")
        host = str(host_value)
        name = str(host_cfg.get("name", host))
        target = local_root / name
        target.mkdir(exist_ok=True, parents=True)
        cmd = [
            "rsync",
            "-avz",
            "--partial",
            "-e",
            ssh.rsync_rsh(),
            f"{ssh.user}@{host}:{results_dir.rstrip('/')}/",
            f"{target}/",
        ]
        return run_local(cmd, check=False, capture=True)

    workers = _pool_workers(parallel, len(fleet))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(worker, host_cfg): host_cfg for host_cfg in fleet}
        for fut in as_completed(futures):
            host_cfg = futures[fut]
            host_value = host_cfg.get("host")
            host = str(host_value) if host_value else "<unknown>"
            name = host_cfg.get("name", host)
            try:
                result = fut.result()
                output = (result.stdout or "").rstrip()
                if result.returncode != 0:
                    print(
                        f"[{host}] RSYNC WARNING (код {result.returncode}):\n{output}",
                        file=sys.stderr,
                    )
                else:
                    print(f"\n[rsync:{name} {host}]\n{output}\n")
            except Exception as exc:  # pragma: no cover - параллельный блок трудно покрыть
                print(f"[{host}] RSYNC ERROR: {exc}", file=sys.stderr)

def main():
    ap = argparse.ArgumentParser(description="VM fleet orchestrator")
    ap.add_argument("--config", "-c", default="fleet.yaml")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Upload and run remote bash script on all VMs")
    p_run.add_argument("--script", default="./bootstrap_worker.sh")
    p_run.add_argument("--image", default=None)
    p_run.add_argument("--cmd-override", default=None)
    p_run.add_argument("--hosts", nargs="*", default=None)
    p_run.add_argument("--parallel", type=int, default=8, help="Максимальное число параллельных подключений")

    p_chk = sub.add_parser("check", help="Check folders, status, disk")
    p_chk.add_argument("--cleanup", action="store_true")
    p_chk.add_argument("--hosts", nargs="*", default=None)
    p_chk.add_argument("--parallel", type=int, default=8, help="Максимальное число параллельных подключений")

    p_sync = sub.add_parser("sync", help="rsync /data/results back")
    p_sync.add_argument("--out", default="results_sync")
    p_sync.add_argument("--hosts", nargs="*", default=None)
    p_sync.add_argument("--parallel", type=int, default=4, help="Максимальное число параллельных подключений")

    args = ap.parse_args()
    cfg = load_cfg(args.config)

    if args.cmd == "run":
        do_run_script(
            cfg,
            args.script,
            image=args.image,
            cmd_override=args.cmd_override,
            hosts=args.hosts,
            parallel=args.parallel,
        )
    elif args.cmd == "check":
        do_check(cfg, cleanup=args.cleanup, hosts=args.hosts, parallel=args.parallel)
    elif args.cmd == "sync":
        do_sync_results(cfg, local_dir=args.out, hosts=args.hosts, parallel=args.parallel)

if __name__ == "__main__":
    main()
