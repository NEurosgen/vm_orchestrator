#!/usr/bin/env python3
import argparse, os, sys, tempfile, subprocess, shlex, yaml, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

def load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ssh_base(cfg):
    key = os.path.expanduser(cfg["ssh"]["key"])
    user = cfg["ssh"]["user"]
    port = cfg["ssh"].get("port", 22)
    strict = cfg["ssh"].get("strict_host_key", "accept-new")
    strict_opt = "-o StrictHostKeyChecking=no" if strict in ("no", "false") else "-o StrictHostKeyChecking=accept-new"
    return key, user, port, strict_opt

def run_local(cmd: str, check=True, capture=False):
    print("[local]", cmd)
    if capture:
        return subprocess.run(cmd, shell=True, check=check, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout
    return subprocess.run(cmd, shell=True, check=check)

def run_ssh(host: str, cmd: str, key: str, user: str, port: int, strict_opt: str, check=True, capture=False):
    ssh_cmd = f'ssh -i {shlex.quote(key)} -p {port} {strict_opt} {shlex.quote(user)}@{shlex.quote(host)} {shlex.quote(cmd)}'
    print(f"[ssh:{host}]", cmd)
    if capture:
        return subprocess.run(ssh_cmd, shell=True, check=check, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout
    return subprocess.run(ssh_cmd, shell=True, check=check)

def scp_to(host: str, local_path: str, remote_path: str, key: str, user: str, port: int, strict_opt: str):
    cmd = f'scp -i {shlex.quote(key)} -P {port} {strict_opt.replace("StrictHostKeyChecking", "UserKnownHostsFile=/dev/null StrictHostKeyChecking")} {shlex.quote(local_path)} {shlex.quote(user)}@{shlex.quote(host)}:{shlex.quote(remote_path)}'
    # (the replace() above disables known_hosts changes for scp; leave as-is if you prefer strict checks)
    print(f"[scp:{host}] {local_path} -> {remote_path}")
    return run_local(cmd, check=True)

def do_run_script(cfg, script_local: str, image=None, cmd_override=None, hosts=None, parallel=8):
    key, user, port, strict_opt = ssh_base(cfg)
    script_remote = cfg["remote"]["script_path"]
    fleet = cfg["fleet"] if not hosts else [h for h in cfg["fleet"] if h["name"] in hosts or h["host"] in hosts]

    def worker(h):
        host = h["host"]
        # Upload script
        scp_to(host, script_local, script_remote, key, user, port, strict_opt)
        # Optional: inject overrides via env
        env = ""
        if image:
            env += f'IMAGE={shlex.quote(image)} '
        if cmd_override:
            env += f'CMD={cmd_override} '  # let the script read CMD if you implement it
        # Make executable + run
        run_ssh(host, f"chmod +x {script_remote}", key, user, port, strict_opt)
        return run_ssh(host, f"{env} bash {script_remote}", key, user, port, strict_opt, capture=True)

    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futs = {ex.submit(worker, h): h for h in fleet}
        for fut in as_completed(futs):
            h = futs[fut]
            host = h["host"]
            try:
                out = fut.result()
                if out and hasattr(out, "stdout"):
                    print(f"\n[{host}] OUTPUT:\n{out.stdout}")
                elif isinstance(out, str):
                    print(f"\n[{host}] OUTPUT:\n{out}")
            except Exception as e:
                print(f"[{host}] ERROR:", e, file=sys.stderr)

def do_check(cfg, cleanup=False, hosts=None, parallel=8):
    key, user, port, strict_opt = ssh_base(cfg)
    folders = cfg["remote"]["folders"]
    max_age = int(cfg["check"].get("max_age_days_tmp", 7))
    list_head = int(cfg["check"].get("list_head", 50))
    fleet = cfg["fleet"] if not hosts else [h for h in cfg["fleet"] if h["name"] in hosts or h["host"] in hosts]

    def one(h):
        host = h["host"]
        cmds = []
        # docker worker status
        cmds.append("docker ps --format '{{.Names}}  {{.Image}}  {{.Status}}' | grep -E 'neurd-worker|$'")
        # disk
        cmds.append("df -h | head -n 2")
        # list folders
        for d in folders:
            cmds.append(f"echo '--- {d} ---'; ls -lah {shlex.quote(d)} | head -n {list_head} || true")
            cmds.append(f"echo 'du -sh {d}'; du -sh {shlex.quote(d)} || true")
        # optional cleanup temp
        if cleanup:
            cmds.append(f"find /data/tmp -type f -mtime +{max_age} -print -delete || true")
        big = " && ".join(f"({c})" for c in cmds)
        return run_ssh(host, big, key, user, port, strict_opt, capture=True)

    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futs = {ex.submit(one, h): h for h in fleet}
        for fut in as_completed(futs):
            h = futs[fut]
            host = h["host"]
            try:
                out = fut.result()
                print(f"\n===== {h['name']} ({host}) =====\n{out}")
            except Exception as e:
                print(f"[{host}] ERROR:", e, file=sys.stderr)

def do_sync_results(cfg, local_dir="results_sync", hosts=None, parallel=4):
    key, user, port, strict_opt = ssh_base(cfg)
    rsync_ssh = f'ssh -i {shlex.quote(os.path.expanduser(cfg["ssh"]["key"]))} -p {port}'
    fleet = cfg["fleet"] if not hosts else [h for h in cfg["fleet"] if h["name"] in hosts or h["host"] in hosts]
    results_dir = "/data/results"

    Path(local_dir).mkdir(parents=True, exist_ok=True)

    def one(h):
        host = h["host"]
        name = h["name"]
        target = Path(local_dir) / name
        target.mkdir(exist_ok=True, parents=True)
        cmd = f'rsync -avz --partial -e "{rsync_ssh}" {cfg["ssh"]["user"]}@{host}:{results_dir}/ {target}/'
        return run_local(cmd, check=False, capture=True)

    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futs = {ex.submit(one, h): h for h in fleet}
        for fut in as_completed(futs):
            h = futs[fut]
            host = h["host"]
            try:
                out = fut.result()
                print(f"\n[rsync:{h['name']} {host}]\n{out}")
            except Exception as e:
                print(f"[{host}] RSYNC ERROR:", e, file=sys.stderr)

def main():
    ap = argparse.ArgumentParser(description="VM fleet orchestrator")
    ap.add_argument("--config", "-c", default="fleet.yaml")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Upload and run remote bash script on all VMs")
    p_run.add_argument("--script", default="./bootstrap_worker.sh")
    p_run.add_argument("--image", default=None)
    p_run.add_argument("--cmd-override", default=None)
    p_run.add_argument("--hosts", nargs="*", default=None)

    p_chk = sub.add_parser("check", help="Check folders, status, disk")
    p_chk.add_argument("--cleanup", action="store_true")
    p_chk.add_argument("--hosts", nargs="*", default=None)

    p_sync = sub.add_parser("sync", help="rsync /data/results back")
    p_sync.add_argument("--out", default="results_sync")
    p_sync.add_argument("--hosts", nargs="*", default=None)

    args = ap.parse_args()
    cfg = load_cfg(args.config)

    if args.cmd == "run":
        do_run_script(cfg, args.script, image=args.image, cmd_override=args.cmd_override, hosts=args.hosts)
    elif args.cmd == "check":
        do_check(cfg, cleanup=args.cleanup, hosts=args.hosts)
    elif args.cmd == "sync":
        do_sync_results(cfg, local_dir=args.out, hosts=args.hosts)

if __name__ == "__main__":
    main()
