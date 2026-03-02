import time
from kubernetes import client, config
from rich.live import Live
from rich.table import Table
from rich.text import Text
from rich import box
import re

CPU_RE = re.compile(r"^(\d+)(n|u|m)?$")
MEM_RE = re.compile(r"^(\d+)(Ki|Mi|Gi)?$")

def parse_cpu(cpu):
    m = CPU_RE.match(cpu.strip())
    if not m:
        return 0.0
    val = int(m.group(1))
    unit = m.group(2)
    if unit == "n":
        return val / 1_000_000_000
    if unit == "u":
        return val / 1_000_000
    if unit == "m":
        return val / 1000
    return float(val)

def parse_mem(mem):
    m = MEM_RE.match(mem.strip())
    if not m:
        return 0.0
    val = int(m.group(1))
    unit = m.group(2)
    if unit == "Ki":
        return val * 1024
    if unit == "Mi":
        return val * 1024**2
    if unit == "Gi":
        return val * 1024**3
    return float(val)

def bar(pct, width=16):
    pct = max(0, min(100, pct))
    filled = int(width * pct / 100)
    empty = width - filled
    color = "green"
    if pct > 85:
        color = "red"
    elif pct > 60:
        color = "yellow"
    t = Text()
    t.append("█" * filled, style=color)
    t.append("░" * empty, style="grey50")
    t.append(f" {pct:5.1f}%")
    return t

def get_node_metadata(core):
    nodes = core.list_node().items
    data = {}

    for n in nodes:
        name = n.metadata.name

        capacity_cpu = parse_cpu(n.status.capacity.get("cpu", "0"))
        alloc_cpu = parse_cpu(n.status.allocatable.get("cpu", "0"))

        capacity_mem = parse_mem(n.status.capacity.get("memory", "0Ki"))
        alloc_mem = parse_mem(n.status.allocatable.get("memory", "0Ki"))

        labels = n.metadata.labels or {}
        taints = n.spec.taints or []

        # pegar labels importantes pro seu cenário EKS
        nodegroup = labels.get("eks.amazonaws.com/nodegroup", "-")
        lifecycle = labels.get("eks.amazonaws.com/capacityType", "-")
        az = labels.get("topology.kubernetes.io/zone", "-")

        taint_str = ",".join([f"{t.key}={t.value}:{t.effect}" for t in taints]) if taints else "-"

        data[name] = {
            "capacity_cpu": capacity_cpu,
            "alloc_cpu": alloc_cpu,
            "capacity_mem": capacity_mem,
            "alloc_mem": alloc_mem,
            "nodegroup": nodegroup,
            "lifecycle": lifecycle,
            "az": az,
            "taints": taint_str
        }

    return data

def main():
    try:
        config.load_kube_config()
    except:
        config.load_incluster_config()

    core = client.CoreV1Api()
    custom = client.CustomObjectsApi()

    node_meta = get_node_metadata(core)

    def render():
        metrics = custom.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes"
        )

        table = Table(title="EKS Node Viewer - Python Edition", box=box.SIMPLE)
        table.add_column("Node")
        table.add_column("NodeGroup")
        table.add_column("Type")
        table.add_column("AZ")
        table.add_column("CPU %")
        table.add_column("Mem %")
        table.add_column("Taints")

        for item in metrics["items"]:
            name = item["metadata"]["name"]
            usage = item["usage"]

            cpu_used = parse_cpu(usage.get("cpu", "0"))
            mem_used = parse_mem(usage.get("memory", "0Ki"))

            meta = node_meta.get(name)
            if not meta:
                continue

            cpu_pct = (cpu_used / meta["alloc_cpu"]) * 100 if meta["alloc_cpu"] > 0 else 0
            mem_pct = (mem_used / meta["alloc_mem"]) * 100 if meta["alloc_mem"] > 0 else 0

            table.add_row(
                name,
                meta["nodegroup"],
                meta["lifecycle"],
                meta["az"],
                bar(cpu_pct),
                bar(mem_pct),
                meta["taints"]
            )

        return table

    with Live(render(), refresh_per_second=1, screen=True) as live:
        while True:
            live.update(render())
            time.sleep(2)

if __name__ == "__main__":
    main()
