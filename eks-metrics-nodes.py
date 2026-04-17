import json
import re
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from kubernetes import client, config
from rich import box
from rich.live import Live
from rich.table import Table
from rich.text import Text

CPU_RE = re.compile(r"^(\d+)(n|u|m)?$")
MEM_RE = re.compile(r"^(\d+)(Ki|Mi|Gi)?$")
INSTANCE_TYPE_RE = re.compile(r"^([a-z0-9-]+)\.([a-z0-9]+)$")

AWS_PRICING_LOCATIONS = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "af-south-1": "Africa (Cape Town)",
    "ap-east-1": "Asia Pacific (Hong Kong)",
    "ap-east-2": "Asia Pacific (Taipei)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "ap-south-2": "Asia Pacific (Hyderabad)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-southeast-3": "Asia Pacific (Jakarta)",
    "ap-southeast-4": "Asia Pacific (Melbourne)",
    "ap-southeast-5": "Asia Pacific (Malaysia)",
    "ap-southeast-7": "Asia Pacific (Thailand)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka)",
    "ca-central-1": "Canada (Central)",
    "ca-west-1": "Canada West (Calgary)",
    "eu-central-1": "EU (Frankfurt)",
    "eu-central-2": "EU (Zurich)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "eu-west-3": "EU (Paris)",
    "eu-south-1": "EU (Milan)",
    "eu-south-2": "EU (Spain)",
    "eu-north-1": "EU (Stockholm)",
    "eu-isoe-west-1": "EU ISOE West",
    "il-central-1": "Israel (Tel Aviv)",
    "me-south-1": "Middle East (Bahrain)",
    "me-central-1": "Middle East (UAE)",
    "mx-central-1": "Mexico (Central)",
    "sa-east-1": "South America (Sao Paulo)",
}

INSTANCE_SIZE_ORDER = [
    "nano",
    "micro",
    "small",
    "medium",
    "large",
    "xlarge",
    "2xlarge",
    "3xlarge",
    "4xlarge",
    "6xlarge",
    "8xlarge",
    "9xlarge",
    "10xlarge",
    "12xlarge",
    "16xlarge",
    "18xlarge",
    "24xlarge",
    "32xlarge",
    "48xlarge",
    "56xlarge",
    "64xlarge",
    "96xlarge",
    "112xlarge",
]

FAMILY_RECOMMENDATIONS = {
    "c": {
        "arm64": ["c7g", "c7gd"],
        "amd64": ["c7a", "c7i", "c6a", "c6i"],
    },
    "m": {
        "arm64": ["m7g", "m7gd"],
        "amd64": ["m7a", "m7i-flex", "m7i", "m6a", "m6i"],
    },
    "r": {
        "arm64": ["r7g"],
        "amd64": ["r7a", "r7i", "r6a", "r6i"],
    },
    "t": {
        "arm64": ["t4g"],
        "amd64": ["t3a", "t3"],
    },
}


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


def parse_instance_type(instance_type):
    if not instance_type or instance_type == "-":
        return None, None
    match = INSTANCE_TYPE_RE.match(instance_type)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def get_family_prefix(family_code):
    for char in family_code:
        if char.isalpha():
            return char
    return None


def get_smaller_size(size):
    if size not in INSTANCE_SIZE_ORDER:
        return None
    index = INSTANCE_SIZE_ORDER.index(size)
    if index == 0:
        return None
    return INSTANCE_SIZE_ORDER[index - 1]


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
    t.append("\u2588" * filled, style=color)
    t.append("\u2591" * empty, style="grey50")
    t.append(f" {pct:5.1f}%")
    return t


def currency(value):
    if value is None:
        return "-"
    return f"${value:.4f}/h"


def percent(value):
    if value is None:
        return "-"
    return f"{value:.1f}%"


class PriceResolver:
    def __init__(self):
        self.pricing = boto3.client("pricing", region_name="us-east-1")
        self.ec2_clients = {}
        self.cache = {}

    def get_hourly_price(self, instance_type, region, lifecycle, az):
        if not instance_type or not region:
            return None

        normalized_lifecycle = (lifecycle or "ON_DEMAND").upper()
        cache_key = (instance_type, region, normalized_lifecycle, az or "-")
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            if normalized_lifecycle == "SPOT":
                price = self._get_spot_price(instance_type, region, az)
            else:
                price = self._get_ondemand_price(instance_type, region)
        except (BotoCoreError, ClientError, KeyError, ValueError, IndexError, json.JSONDecodeError):
            price = None

        self.cache[cache_key] = price
        return price

    def _get_ec2_client(self, region):
        if region not in self.ec2_clients:
            self.ec2_clients[region] = boto3.client("ec2", region_name=region)
        return self.ec2_clients[region]

    def _get_spot_price(self, instance_type, region, az):
        if not az:
            return None

        ec2 = self._get_ec2_client(region)
        result = ec2.describe_spot_price_history(
            InstanceTypes=[instance_type],
            AvailabilityZone=az,
            ProductDescriptions=["Linux/UNIX"],
            MaxResults=1,
        )
        history = result.get("SpotPriceHistory", [])
        if not history:
            return None
        return float(history[0]["SpotPrice"])

    def _get_ondemand_price(self, instance_type, region):
        location = AWS_PRICING_LOCATIONS.get(region)
        if not location:
            return None

        result = self.pricing.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            ],
            MaxResults=1,
        )

        price_list = result.get("PriceList", [])
        if not price_list:
            return None

        payload = json.loads(price_list[0])
        on_demand_terms = payload["terms"]["OnDemand"]
        for term in on_demand_terms.values():
            for dimension in term["priceDimensions"].values():
                price_per_unit = dimension["pricePerUnit"].get("USD")
                if price_per_unit:
                    return float(price_per_unit)
        return None


def build_recommendation(meta, cpu_pct, mem_pct, price_resolver):
    current_type = meta["instance_type"]
    current_price = meta["hourly_price"]
    lifecycle = meta["lifecycle"]
    region = meta["region"]
    az = meta["az"]
    arch = meta["arch"]

    family_code, size = parse_instance_type(current_type)
    if not family_code or not size or current_price is None:
        return {"instance_type": "-", "price": None, "savings_pct": None, "reason": "sem dados"}

    family_prefix = get_family_prefix(family_code)
    if family_prefix not in FAMILY_RECOMMENDATIONS:
        return {"instance_type": "-", "price": None, "savings_pct": None, "reason": "familia sem regra"}

    usage_peak = max(cpu_pct, mem_pct)
    candidate_sizes = [size]
    smaller_size = get_smaller_size(size)
    if usage_peak < 35 and smaller_size:
        candidate_sizes.insert(0, smaller_size)

    candidates = []
    for candidate_family in FAMILY_RECOMMENDATIONS[family_prefix].get(arch, []):
        for candidate_size in candidate_sizes:
            candidate_type = f"{candidate_family}.{candidate_size}"
            if candidate_type == current_type:
                continue

            candidate_price = price_resolver.get_hourly_price(
                instance_type=candidate_type,
                region=region if region != "-" else None,
                lifecycle=lifecycle,
                az=az if az != "-" else None,
            )
            if candidate_price is None or candidate_price >= current_price:
                continue

            reason_parts = []
            if candidate_size != size:
                reason_parts.append("baixo uso")
            if candidate_family != family_code:
                if arch == "arm64" and candidate_family.endswith("g"):
                    reason_parts.append("Graviton")
                else:
                    reason_parts.append("geracao nova")
            if cpu_pct > mem_pct + 15:
                reason_parts.append("cpu alta")
            elif mem_pct > cpu_pct + 15:
                reason_parts.append("mem alta")

            candidates.append(
                {
                    "instance_type": candidate_type,
                    "price": candidate_price,
                    "savings_pct": ((current_price - candidate_price) / current_price) * 100,
                    "reason": ", ".join(reason_parts) if reason_parts else "menor custo/h",
                }
            )

    if not candidates:
        return {"instance_type": "-", "price": None, "savings_pct": None, "reason": "atual ok"}

    return max(candidates, key=lambda item: item["savings_pct"])


def get_node_metadata(core, price_resolver):
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

        nodegroup = labels.get("eks.amazonaws.com/nodegroup", "-")
        lifecycle = labels.get("eks.amazonaws.com/capacityType", "ON_DEMAND")
        az = labels.get("topology.kubernetes.io/zone", "-")
        region = labels.get("topology.kubernetes.io/region", "-")
        arch = labels.get("kubernetes.io/arch", "amd64")
        instance_type = labels.get(
            "node.kubernetes.io/instance-type",
            labels.get("beta.kubernetes.io/instance-type", "-"),
        )

        taint_str = ",".join(
            f"{t.key}={t.value}:{t.effect}" for t in taints
        ) if taints else "-"

        hourly_price = price_resolver.get_hourly_price(
            instance_type=instance_type if instance_type != "-" else None,
            region=region if region != "-" else None,
            lifecycle=lifecycle,
            az=az if az != "-" else None,
        )

        data[name] = {
            "capacity_cpu": capacity_cpu,
            "alloc_cpu": alloc_cpu,
            "capacity_mem": capacity_mem,
            "alloc_mem": alloc_mem,
            "nodegroup": nodegroup,
            "lifecycle": lifecycle,
            "az": az,
            "region": region,
            "arch": arch,
            "instance_type": instance_type,
            "taints": taint_str,
            "hourly_price": hourly_price,
        }

    return data


def main():
    try:
        config.load_kube_config()
    except Exception:
        config.load_incluster_config()

    core = client.CoreV1Api()
    custom = client.CustomObjectsApi()
    price_resolver = PriceResolver()

    def render():
        node_meta = get_node_metadata(core, price_resolver)
        metrics = custom.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes",
        )

        table = Table(title="EKS Node Viewer - Python Edition", box=box.SIMPLE)
        table.add_column("Node")
        table.add_column("NodeGroup")
        table.add_column("Type")
        table.add_column("EC2")
        table.add_column("AZ")
        table.add_column("CPU %")
        table.add_column("Mem %")
        table.add_column("EC2 $/h")
        table.add_column("Uso $/h")
        table.add_column("Recommendation")
        table.add_column("Save")
        table.add_column("Why")
        table.add_column("Taints")

        total_hourly = 0.0
        total_used_hourly = 0.0

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

            hourly_price = meta["hourly_price"]
            used_ratio = max(cpu_pct, mem_pct) / 100
            used_hourly = hourly_price * used_ratio if hourly_price is not None else None
            recommendation = build_recommendation(meta, cpu_pct, mem_pct, price_resolver)

            if hourly_price is not None:
                total_hourly += hourly_price
            if used_hourly is not None:
                total_used_hourly += used_hourly

            table.add_row(
                name,
                meta["nodegroup"],
                meta["lifecycle"],
                meta["instance_type"],
                meta["az"],
                bar(cpu_pct),
                bar(mem_pct),
                currency(hourly_price),
                currency(used_hourly),
                recommendation["instance_type"],
                percent(recommendation["savings_pct"]),
                recommendation["reason"],
                meta["taints"],
            )

        table.add_section()
        table.add_row(
            "TOTAL",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            currency(total_hourly),
            currency(total_used_hourly),
            "-",
            "-",
            "-",
            "-",
            style="bold cyan",
        )

        return table

    with Live(render(), refresh_per_second=1, screen=True) as live:
        while True:
            live.update(render())
            time.sleep(2)


if __name__ == "__main__":
    main()
