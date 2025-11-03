#!/usr/bin/env python3
"""Высокоуровневый пайплайн оркестрации обработки мешей на удалённых ВМ."""

from __future__ import annotations

import argparse
import queue
import shlex
import time
from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import vm_orchestrator


@dataclass(frozen=True)
class RemotePaths:
    """Пути на удалённой машине, используемые пайплайном."""

    meshes: str
    results: str
    logs: str


@dataclass
class Manifest:
    """Хранит информацию о завершённых и проваленных задачах."""

    done_path: Path
    failed_path: Optional[Path] = None
    done: Set[str] = field(default_factory=set)
    failed: Set[str] = field(default_factory=set)

    @classmethod
    def load(cls, done_path: Path, failed_path: Optional[Path]) -> "Manifest":
        return cls(
            done_path=done_path,
            failed_path=failed_path,
            done=_read_manifest(done_path),
            failed=_read_manifest(failed_path) if failed_path else set(),
        )

    def mark_done(self, mesh_id: str) -> None:
        if mesh_id in self.done:
            return
        self.done.add(mesh_id)
        self._append(self.done_path, mesh_id)

    def mark_failed(self, mesh_id: str) -> None:
        if not self.failed_path or mesh_id in self.failed:
            return
        self.failed.add(mesh_id)
        self._append(self.failed_path, mesh_id)

    @staticmethod
    def _append(path: Path, mesh_id: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(f"{mesh_id}\n")


@dataclass
class RemoteWorker:
    """Помощник для взаимодействия с конкретной удалённой машиной."""

    name: str
    host: str
    ssh: vm_orchestrator.SSHParams
    paths: RemotePaths
    mesh_pattern: str

    def ensure_ready(self, docker_image: Optional[str]) -> None:
        quoted_meshes = shlex.quote(self.paths.meshes)
        quoted_results = shlex.quote(self.paths.results)
        quoted_logs = shlex.quote(self.paths.logs)
        vm_orchestrator.run_ssh(
            self.host,
            f"mkdir -p {quoted_meshes} {quoted_results} {quoted_logs}",
            self.ssh,
            check=True,
            capture=False,
        )
        if docker_image:
            vm_orchestrator.run_ssh(
                self.host,
                f"docker pull {shlex.quote(docker_image)}",
                self.ssh,
                check=True,
                capture=False,
            )

    def list_meshes(self) -> List[str]:
        return self._list_dir(self.paths.meshes, pattern=self.mesh_pattern)

    def list_results(self) -> List[str]:
        return self._list_dir(self.paths.results)

    def upload_mesh(self, mesh_path: Path) -> None:
        vm_orchestrator.scp_to(
            self.host,
            mesh_path,
            f"{self.paths.meshes.rstrip('/')}/",
            self.ssh,
        )

    def download_results(self, local_root: Path) -> None:
        local_host_dir = local_root / self.name
        local_host_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            "rsync",
            "-avz",
            "--partial",
            "-e",
            self.ssh.rsync_rsh(),
            f"{self.ssh.user}@{self.host}:{self.paths.results.rstrip('/')}/",
            f"{local_host_dir}/",
        ]
        vm_orchestrator.run_local(cmd, check=False, capture=False)

    def clear_remote(self) -> None:
        meshes_dir = shlex.quote(self.paths.meshes.rstrip("/"))
        results_dir = shlex.quote(self.paths.results.rstrip("/"))
        vm_orchestrator.run_ssh(
            self.host,
            f"find {results_dir} -mindepth 1 -delete || true",
            self.ssh,
            check=False,
            capture=False,
        )
        vm_orchestrator.run_ssh(
            self.host,
            f"find {meshes_dir} -type f -name {shlex.quote(self.mesh_pattern)} -delete || true",
            self.ssh,
            check=False,
            capture=False,
        )

    def _list_dir(self, remote_dir: str, *, pattern: Optional[str] = None) -> List[str]:
        glob = f"{remote_dir.rstrip('/')}/{pattern or '*'}"
        cmd = f"ls -1 {shlex.quote(glob)} 2>/dev/null"
        result = vm_orchestrator.run_ssh(
            self.host,
            cmd,
            self.ssh,
            check=False,
            capture=True,
        )
        if result.returncode != 0:
            return []
        output = (result.stdout or "").strip()
        if not output:
            return []
        return [line.strip() for line in output.splitlines() if line.strip()]


def _read_manifest(path: Optional[Path]) -> Set[str]:
    if not path or not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as fh:
        return {line.strip().split()[0] for line in fh if line.strip()}


def _build_mesh_queue(mesh_dir: Path, pattern: str, manifest: Manifest) -> queue.Queue[Path]:
    if not mesh_dir.exists():
        raise FileNotFoundError(f"Каталог с мешами не найден: {mesh_dir}")
    files = sorted(mesh_dir.glob(pattern))
    tasks: queue.Queue[Path] = queue.Queue()
    for mesh in files:
        spine_id = mesh.stem
        if spine_id in manifest.done:
            continue
        if spine_id in manifest.failed:
            continue
        tasks.put(mesh)
    return tasks


def _select_fleet(cfg: Mapping[str, object], requested: Optional[Iterable[str]]) -> List[Mapping[str, object]]:
    fleet_cfg = cfg.get("fleet", [])
    if not isinstance(fleet_cfg, list):
        raise ValueError("Раздел 'fleet' в конфигурации должен быть списком")
    if not requested:
        return [entry for entry in fleet_cfg if isinstance(entry, Mapping)]

    requested_set = {item.lower() for item in requested}
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
    return selected


def _build_workers(
    cfg: MutableMapping[str, object],
    ssh: vm_orchestrator.SSHParams,
    *,
    hosts: Optional[Iterable[str]],
    mesh_pattern: str,
    remote_paths: RemotePaths,
) -> List[RemoteWorker]:
    fleet = _select_fleet(cfg, hosts)
    workers: List[RemoteWorker] = []
    for entry in fleet:
        host_value = entry.get("host")
        if not host_value:
            raise ValueError("Каждая запись во 'fleet' должна содержать поле 'host'")
        host = str(host_value)
        name = str(entry.get("name", host))
        workers.append(
            RemoteWorker(
                name=name,
                host=host,
                ssh=ssh,
                paths=remote_paths,
                mesh_pattern=mesh_pattern,
            )
        )
    if not workers:
        raise ValueError("Не найдено подходящих хостов во флите")
    return workers


def work_cycle(
    workers: List[RemoteWorker],
    tasks: queue.Queue[Path],
    manifest: Manifest,
    assignments: Dict[str, Path],
    download_dir: Path,
) -> None:
    for worker in workers:
        results = worker.list_results()
        if results:
            assigned = assignments.pop(worker.name, None)
            worker.download_results(download_dir)
            worker.clear_remote()
            if assigned is not None:
                manifest.mark_done(assigned.stem)
            continue

        if worker.list_meshes():
            # Машина занята обработкой текущего меша.
            continue

        if worker.name in assignments:
            # Меш ещё назначен, но файлов нет — считаем провалом.
            failed_mesh = assignments.pop(worker.name)
            manifest.mark_failed(failed_mesh.stem)

        try:
            mesh = tasks.get_nowait()
        except queue.Empty:
            continue
        worker.clear_remote()
        worker.upload_mesh(mesh)
        assignments[worker.name] = mesh


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Оркестратор обработки мешей на ВМ")
    ap.add_argument("--config", "-c", default="fleet.yaml", help="Путь к конфигурации флота")
    ap.add_argument("--meshes", required=True, help="Каталог с входными мешами")
    ap.add_argument("--pattern", default="*.off", help="Глоб-шаблон для поиска мешей")
    ap.add_argument("--results", default="results", help="Локальный каталог для выгрузки результатов")
    ap.add_argument("--manifest", default="manifest.txt", help="Файл с завершёнными заданиями")
    ap.add_argument("--failed-manifest", default=None, help="Файл с проваленными заданиями")
    ap.add_argument("--remote-meshes", default="/data/meshes")
    ap.add_argument("--remote-results", default="/data/results")
    ap.add_argument("--remote-logs", default="/data/logs")
    ap.add_argument("--docker-image", default=None, help="Docker-образ для предварительной загрузки")
    ap.add_argument("--hosts", nargs="*", default=None, help="Ограничить список машин по имени/хосту")
    ap.add_argument("--poll", type=float, default=30.0, help="Пауза между циклами опроса, секунды")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    cfg = vm_orchestrator.load_cfg(args.config)
    ssh = vm_orchestrator.SSHParams.from_config(cfg)
    remote_paths = RemotePaths(
        meshes=args.remote_meshes,
        results=args.remote_results,
        logs=args.remote_logs,
    )
    manifest = Manifest.load(Path(args.manifest), Path(args.failed_manifest) if args.failed_manifest else None)
    mesh_dir = Path(args.meshes)
    tasks = _build_mesh_queue(mesh_dir, args.pattern, manifest)
    workers = _build_workers(cfg, ssh, hosts=args.hosts, mesh_pattern=args.pattern, remote_paths=remote_paths)

    for worker in workers:
        worker.ensure_ready(args.docker_image)

    download_dir = Path(args.results)
    download_dir.mkdir(parents=True, exist_ok=True)

    assignments: Dict[str, Path] = {}
    try:
        while True:
            work_cycle(workers, tasks, manifest, assignments, download_dir)
            if assignments or not tasks.empty():
                time.sleep(args.poll)
            else:
                break
    except KeyboardInterrupt:
        print("[warn] остановлено пользователем")


if __name__ == "__main__":
    main()

