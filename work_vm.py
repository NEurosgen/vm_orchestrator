import vm_orchestrator

import queue


def pull_docker_image(VM,docker_image):
    return VM.run(cmd = f'docker pull {docker_image}')


def create_dirs(VM , meshes = 'meshes',results = 'results'):
    return VM.run(cmd = f'mkdir -p {meshes} {results} , logs')



def init_vm(VM,docker_image,meshes = 'meshes',results='results'):
    create_dirs(VM,meshes,results)
    pull_docker_image(docker_image=docker_image,VM = VM)

from typing import Set, Any, Dict
def read_manifest(manifest_path: Path) -> Set[str]:
    if not manifest_path.exists():
        return set()
    with open(manifest_path, "r", encoding="utf-8") as f:
        return {line.strip().split()[0] for line in f if line.strip()}
def load_meshses(VM,mesh_dir_path , pattern = '*.off',out_dir)->queue:
    files = sorted(mesh_dir_path.glob(pattern))
    if not files:
        print(f"[!] no files matched: {mesh_dir_path}/{pattern}")
        return

    done = read_manifest(out_dir)
    done_failed = read_manifest(out_dir/Path("failed.txt"))
    print(f"[i] manifest: {manifest_path} (loaded {len(done)} ids)")
    print(f"[i] found {len(files)} mesh(es) in: {in_dir}")


    failed_log = out_dir / "failed.txt"

    processed = skipped = failed = 0
    total = len(files)

    for i, mesh_path in enumerate(files, 1):
        spine_id = mesh_path.stem
        if spine_id in done :
            print(f"[{i}/{total}] [=] skip by manifest: {spine_id}")
            skipped += 1
            continue
        if spine_id in done_failed:
            print(f"[{i}/{total}] [=] skip by failed_manifest: {spine_id}")
            skipped += 1
            continue

    return queue.Queue()

def check_machine(VM :vm_orchestrator.VMOrchestrator, targets):
    if len(VM.listdir(targets=targets,remote_dir='meshes'))  == 1:
        if len(VM.listdir(targets=targets,remote_dir='results'))  == 1:
            return 'end_work'
        else :
            return 'workning' 
    return 'free'
from pathlib import Path
def download_seg(VM:vm_orchestrator,host,download_path):
    VM.get(remote_path='results',targets = [host],local_dir = download_path)
def del_seg_and_mesh(VM:vm_orchestrator,host):
    VM.delete(remote_path = 'meshes',pattern = '*.off',targets = [host])
    VM.delete(remote_path = 'results',recursive=True,targets = [host])
def upload_mesh(VM:vm_orchestrator,host,mesh:Path):
    VM.put(local_path = mesh,remote_path = 'meshes',targets = [host])
    
def work_cycle(VM:vm_orchestrator.VMOrchestrator,meshes:queue.Queue):
    hosts = VM.hosts()
    for h in hosts:
        status  = check_machine(VM,targets=[h])
        if status == 'free':
            upload_mesh(VM = VM,mesh = meshes.get(),host = h)
            continue
        if status == 'end_work':
            download_seg(VM = VM,host = h,download_path = download_path)
            del_seg_and_mesh(VM,h)
            continue
        if status == 'workning':
            continue
        

def main(docker_image,):
    VM = vm_orchestrator.VMOrchestrator(conf)
    init_vm(VM,docker_image)
    meshses_path = load_meshses()
    while True:
        sleep(???)
        work_cycle(VM,meshes = meshses_path)
        


