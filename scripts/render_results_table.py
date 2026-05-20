"""Build the comparison table in `report/spark_results_table.md`.

Reads every `*.json` file in `results/`, groups runs by mode
(`local` / `cluster` / `yarn`), and renders a markdown table comparing
wall-clock, row counts, and top aggregation rows side by side.

Usage:
    python3 scripts/render_results_table.py
"""

import glob
import json
import os
from collections import defaultdict
from typing import Dict, List


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(REPO_ROOT, "results")
REPORT_PATH = os.path.join(REPO_ROOT, "report", "spark_results_table.md")


def load_runs(results_dir: str) -> Dict[str, List[dict]]:
    """Return all run JSON files grouped by mode label."""
    by_mode: Dict[str, List[dict]] = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(results_dir, "*.json"))):
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        by_mode[payload["mode"]].append(payload)
    return by_mode


def latest(runs: List[dict]) -> dict:
    """Pick the most recent run for a given mode (by wall_clock entry order)."""
    return runs[-1]


def fmt_sec(seconds: float) -> str:
    """Render seconds as 'NNN.NN sec (≈X min)'."""
    minutes = seconds / 60
    return f"{seconds:.2f} sec (≈ {minutes:.1f} min)"


def render_table(runs_by_mode: Dict[str, List[dict]]) -> str:
    """Render the markdown comparison report."""
    lines: List[str] = []
    lines.append("# Spark — таблица сравнения режимов запуска\n")
    lines.append("Файл сгенерирован автоматически из `results/*.json` ")
    lines.append("через `scripts/render_results_table.py`.\n")

    if not runs_by_mode:
        lines.append("\n_Нет данных: запустите `jobs/spark_job.py` хотя бы в двух режимах._\n")
        return "".join(lines)

    # Headline timing table.
    lines.append("\n## Время выполнения\n\n")
    lines.append("| Режим | Wall-clock | Строк в датасете | Файл |\n")
    lines.append("|---|---|---:|---|\n")
    for mode in sorted(runs_by_mode):
        run = latest(runs_by_mode[mode])
        lines.append(
            f"| `{run['master']}` | {fmt_sec(run['wall_clock_sec'])} | "
            f"{run['total_rows']:,} | `results/{mode}_*.json` |\n"
        )

    # Side-by-side aggregation: by country.
    if all(runs_by_mode[mode][-1].get("by_country") for mode in runs_by_mode):
        lines.append("\n## groupBy(country): events / unique users / revenue\n\n")
        sample_mode = next(iter(runs_by_mode))
        countries = [row["country"] for row in latest(runs_by_mode[sample_mode])["by_country"]]
        header = "| country |" + "".join(f" events ({m}) |" for m in sorted(runs_by_mode)) + "\n"
        sep = "|---|" + "---|" * len(runs_by_mode) + "\n"
        lines.append(header)
        lines.append(sep)
        for c in countries:
            row = f"| {c} |"
            for m in sorted(runs_by_mode):
                by_country = latest(runs_by_mode[m])["by_country"]
                events = next((r["events"] for r in by_country if r["country"] == c), "—")
                row += f" {events:,} |" if isinstance(events, int) else f" {events} |"
            lines.append(row + "\n")

    # Speedup / slowdown.
    if len(runs_by_mode) >= 2:
        lines.append("\n## Относительная скорость\n\n")
        local = runs_by_mode.get("local")
        cluster = runs_by_mode.get("cluster") or runs_by_mode.get("yarn")
        if local and cluster:
            t_local = latest(local)["wall_clock_sec"]
            t_cluster = latest(cluster)["wall_clock_sec"]
            ratio = t_local / t_cluster if t_cluster else 0
            faster = "cluster" if t_cluster < t_local else "local"
            lines.append(f"- local:   **{fmt_sec(t_local)}**\n")
            lines.append(f"- cluster: **{fmt_sec(t_cluster)}**\n")
            lines.append(f"- быстрее: **{faster}**, отношение local/cluster = **{ratio:.2f}x**\n")

    return "".join(lines)


def main() -> None:
    runs_by_mode = load_runs(RESULTS_DIR)
    report = render_table(runs_by_mode)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"Wrote {REPORT_PATH}")
    print(f"Modes found: {list(runs_by_mode)}")


if __name__ == "__main__":
    main()
