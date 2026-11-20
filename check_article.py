#!/usr/bin/env python3
"""Every number the article quotes, recomputed from the committed logs.

    python3 check_article.py ../dapr-vs-temporal-workflow-chaos.html

Exits non-zero and names each claim whose text no longer matches the runs.
"""
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).parent
article = Path(sys.argv[1]).read_text(encoding="utf-8")
text = re.sub(r"<[^>]+>", "", article)
failures = []
checked = 0


def claim(description, condition):
    global checked
    checked += 1
    if not condition:
        failures.append(description)


def says(fragment):
    return fragment in text


def log(name):
    return (ROOT / name).read_text(encoding="utf-8")


def steps(name):
    rows = {}
    for engine, link, ms, rt in re.findall(
            r"^STEP (\w+)\s+delay-on=(\S+)\s+ms/step=\s*([\d.]+) round-trips/step=(\S+)", log(name), re.M):
        rows[(engine, link)] = (float(ms), None if "-" == rt else float(rt))
    return rows


def one(x):
    return f"{x:.1f}"


# --- latency table and prose: test-results.log -----------------------------------------------------------------------
main = steps("test-results.log")
for engine, link, label in [("Dapr", "none", "Dapr, none"), ("Dapr", "dapr-app", "Dapr, app to sidecar"),
                            ("Dapr", "dapr-store", "Dapr, sidecar to Redis"), ("Temporal", "none", "Temporal, none"),
                            ("Temporal", "temporal-app", "Temporal, worker to server"),
                            ("Temporal", "temporal-store", "Temporal, server to PostgreSQL")]:
    ms, rt = main[(engine, link)]
    ms_text = f"{ms:.0f}" if ms > 100 else one(ms)
    row = re.search(re.escape(label) + r"</td><td>([^<]*)</td><td>([^<]*)</td>", article)
    claim(f"table row {label}: ms {ms_text}", row and row.group(1) == ms_text)
    claim(f"table row {label}: round trips {rt}", row and row.group(2) == ("" if rt is None else one(rt)))

t_base = one(main[("Temporal", "none")][0])
claim(f"dev-server paragraph quotes the real server's {t_base} ms", says(f"against {t_base} ms on the real server"))
t_store_ms, t_store_rt = main[("Temporal", "temporal-store")]
claim(f"prose: 5 ms store delay -> {t_store_ms:.0f} ms, {t_store_rt:.0f} round trips",
      says(f"the step took {t_store_ms:.0f} ms, the equivalent of {t_store_rt:.0f} round trips"))
for (engine, link) in [("Dapr", "dapr-app"), ("Temporal", "temporal-app")]:
    claim(f"'about two round trips' on the app link holds for {engine}", 1.5 <= main[(engine, link)][1] <= 2.5)
claim("'about four round trips' to Redis", 3.5 <= main[("Dapr", "dapr-store")][1] <= 4.5)

store = dict(re.findall(r"^STORE \w+\s+(\S+)\s+.*-> ([\d.]+) per step", log("test-results.log"), re.M))
claim(f"Redis commands per step {float(store['redis-commands']):.0f}",
      says(f"counted {float(store['redis-commands']):.0f} commands per Dapr step"))
sql = one(float(store["sql-statements"]))
claim(f"SQL statements per step {sql} quoted three times", 3 <= text.count(f"{sql} statements") + text.count(f"{sql} SQL statements"))
claim(f"'more than twice' the statements", t_store_rt > 2 * float(store["sql-statements"]))

two = steps("runs/step-cost-delay-2ms.log")[("Temporal", "temporal-store")]
claim(f"2 ms run: {two[0]:.0f} ms, {two[1]:.0f} round trips", says(f"with 2 ms it took {two[0]:.0f} ms, the equivalent of {two[1]:.0f}"))
claim("2 ms and 5 ms give different round-trip counts (not proportional)", abs(two[1] - t_store_rt) > 5)
dev = steps("runs/step-cost-dev-server.log")[("Temporal", "none")][0]
claim(f"dev server {one(dev)} ms per step", says(f"took {one(dev)} ms per step"))

claim("24 tests in the committed run", re.search(r"Tests run: 24, Failures: 0, Errors: 0", log("test-results.log"))
      and says("runs 24 tests"))

# --- charges: every RESULT and OBSERVED line in the committed runs ---------------------------------------------------
results = []  # (engine, scenario, outage_ms, charges, recovery)
for name in ["test-results.log", "runs/repeat-blip-hung.log", "runs/repeat-deploy.log"]:
    for engine, order, outcome, charges, recovery in re.findall(
            r"^RESULT (\w+)\s+(\S+)\s+Result\[outcome=(\w+), charges=(\d+), refunds=\d+, recoverySeconds=([\d.]+)\]",
            log(name), re.M):
        results.append((engine, order, outcome, int(charges), float(recovery)))
observed = []
for name in ["runs/explore-3-rounds-temporal.log", "runs/explore-3-rounds-both.log"]:  # explore-3-rounds.log was a copy
    for engine, scenario, outage, outcome, charges, recovery in re.findall(
            r"^OBSERVED (\w+)\s+(\S+)\s+(\d+)ms outcome=(\S+)\s+charges=(\d+) recovery=([\d.]+)s", log(name), re.M):
        observed.append((engine, scenario, int(outage), int(charges), float(recovery)))


def charges_of(order):
    return [c for e, o, _, c, _ in results if o == order]


def observed_charges(engine, scenario, outage):
    return [c for e, s, o, c, _ in observed if e == engine and s == scenario and o == outage]


def always(values, expected):
    return values and set(values) == {expected}


claim("300 ms blip: Dapr 2 in every run", always(charges_of("dapr-blip"), 2))
claim("300 ms blip: Temporal 1 in every run", always(charges_of("temporal-blip"), 1))
claim("300 ms blip repeated at least six times", len(charges_of("dapr-blip")) >= 6 and len(charges_of("temporal-blip")) >= 6)
claim("hang: Dapr 1, Temporal 3 in every run, at least six each",
      always(charges_of("dapr-hung"), 1) and always(charges_of("temporal-hung"), 3)
      and len(charges_of("dapr-hung")) >= 6 and len(charges_of("temporal-hung")) >= 6)
claim("hang: Temporal saga FAILED every time",
      {out for e, o, out, _, _ in results if o == "temporal-hung"} == {"FAILED"})
for scenario in ["app-link-cut", "app-link-blackhole", "engine-restart", "worker-crash"]:
    claim(f"Dapr {scenario} 2 s: 2 charges in all six explore runs", always(observed_charges("Dapr", scenario, 2000), 2)
          and len(observed_charges("Dapr", scenario, 2000)) >= 3)
for scenario in ["app-link-cut", "app-link-blackhole"]:
    claim(f"Temporal {scenario} 2 s: 1 charge, six runs", always(observed_charges("Temporal", scenario, 2000), 1)
          and len(observed_charges("Temporal", scenario, 2000)) >= 6)
    claim(f"Temporal {scenario} 10 s: 2 charges, six runs", always(observed_charges("Temporal", scenario, 10000), 2))
for engine in ["Dapr", "Temporal"]:
    for scenario in ["store-link-cut", "store-link-blackhole"]:
        if (engine, scenario) != ("Temporal", "store-link-blackhole"):
            claim(f"{engine} {scenario} 2 s: 1 charge", always(observed_charges(engine, scenario, 2000), 1))
pg_blackhole = observed_charges("Temporal", "store-link-blackhole", 2000) + [
    int(c) for name in ["test-results.log", "runs/repeat-store.log", "runs/clean-copy-run.log"]
    for c in re.findall(r"^RESULT Temporal\s+temporal-store-blackhole\s+Result\[outcome=\w+, charges=(\d+)", log(name), re.M)]
counts_pg = Counter(pg_blackhole)
claim(f"2 s PostgreSQL blackhole: once in {counts_pg[1]} of {len(pg_blackhole)}, twice in {counts_pg[2]}",
      says(f"once in {counts_pg[1]} of {len(pg_blackhole)} runs, twice in {counts_pg[2]}"))

restarts = observed_charges("Temporal", "engine-restart", 2000) + observed_charges("Temporal", "engine-restart-done-during", 2000)
counts = Counter(restarts)
claim(f"Temporal 2 s restart: twelve runs, eight once, four twice (got {dict(counts)})",
      len(restarts) == 12 and counts[1] == 8 and counts[2] == 4 and says("In twelve runs it charged once eight times and twice four times"))
pg_cut = observed_charges("Temporal", "store-link-cut", 10000)
claim(f"10 s PostgreSQL cut: twice in five of six (got {pg_cut[-6:]})",
      Counter(pg_cut[-6:])[2] == 5 and says("charged twice in five runs of six"))
claim("10 s PostgreSQL blackhole: once every run", always(observed_charges("Temporal", "store-link-blackhole", 10000), 1))

restart_recovery = [r for e, s, o, _, r in observed if e == "Temporal" and s.startswith("engine-restart") and o == 2000]
claim(f"restart recovery 'one to seven seconds' ({min(restart_recovery)}-{max(restart_recovery)})",
      1.0 <= min(restart_recovery) and max(restart_recovery) <= 7.0 and says("from one to seven seconds"))

crash_dapr = [r for e, s, o, _, r in observed if e == "Dapr" and s == "worker-crash" and o == 2000] + \
             [r for e, o, _, _, r in results if o == "dapr-crash"]
crash_temporal = [r for e, s, o, _, r in observed if e == "Temporal" and s == "worker-crash" and o == 2000] + \
                 [r for e, o, _, _, r in results if o == "temporal-crash"]
claim(f"Dapr crash recovery 'one to five' ({min(crash_dapr):.1f}-{max(crash_dapr):.1f})",
      1.0 <= min(crash_dapr) and max(crash_dapr) < 5.5 and says("1 to 5 s"))
claim(f"Temporal crash recovery 'nine to fourteen' ({min(crash_temporal):.1f}-{max(crash_temporal):.1f})",
      9.0 <= min(crash_temporal) and max(crash_temporal) < 14.5 and says("9 to 14 s"))

# --- the article's own tables say what the runs say ------------------------------------------------------------------
def row(label):
    match = re.search(r"<tr><td>" + re.escape(label) + r"</td><td>(.*?)</td><td>(.*?)</td></tr>", article)
    return None if None is match else tuple(re.sub(r"<[^>]+>", "", cell) for cell in match.groups())


for label, dapr, temporal in [("App link cut for 300 ms", "2", "1"), ("App link cut for 2 s", "2", "1"),
                              ("App link blackholed for 2 s", "2", "1"), ("Dapr sidecar restarted (2 s down)", "2", "see below"),
                              ("App link cut for 10 s, longer than the 5 s timeout", "2", "2"),
                              ("Charges", "2", "2"), ("From new worker to saga done", "1 to 5 s", "9 to 14 s")]:
    claim(f"table row '{label}' says {dapr} / {temporal}", row(label) == (dapr, temporal))
hang = re.search(r"charge hangs for 25 s.*?</table>", article, re.S).group(0)
claim("hang table: Dapr 1 charge, Temporal 3", "<td>Charges</td><td>1</td><td><strong>3</strong></td>" in hang)

# --- remedies: RemedyTest in test-results.log ------------------------------------------------------------------------
def result(order):
    return next((r for r in results if r[1] == order), None)


for order, outcome, charges in [("safe-dapr-blip", "shipped", 1), ("safe-dapr-crash", "shipped", 1),
                                ("safe-temporal-crash", "shipped", 1), ("safe-temporal-long-cut", "shipped", 1),
                                ("safe-temporal-hung", "cancelled", 1), ("puzzle-naive", "FAILED", 3),
                                ("puzzle-safe", "cancelled", 1), ("safe-dapr-deploy", "shipped", 1),
                                ("dapr-compensation-outage", "cancelled", 1),
                                ("temporal-compensation-outage", "cancelled", 1)]:
    r = result(order)
    claim(f"{order}: {outcome}, {charges} charge(s)", r and r[2] == outcome and r[3] == charges)
for label, naive, safe in [("Dapr, rolling deploy", "2 charges", "1 charge"), ("Dapr, 300 ms blip", "2 charges", "1 charge"), ("Dapr, worker crash", "2 charges", "1 charge"),
                           ("Temporal, worker crash", "2 charges", "1 charge"),
                           ("Temporal, app link cut for 10 s", "2 charges", "1 charge"),
                           ("Temporal, charge hangs for 25 s", "3 charges, failed, nothing refunded", "1 charge, refunded, cancelled")]:
    claim(f"remedy table row '{label}'", row(label) == (naive, safe))
heartbeat_only = result("heartbeat-crash")[4]
sticky = result("sticky-crash")[4]
claim(f"heartbeat-only recovery {one(heartbeat_only)} s quoted",
      f"<td>Heartbeat timeout 1 s</td><td>{one(heartbeat_only)} s</td>" in article)
claim(f"heartbeat + sticky recovery {one(sticky)} s quoted",
      f"<td>Heartbeat timeout 1 s and sticky queue timeout 1 s</td><td>{one(sticky)} s</td>" in article)
claim(f"heartbeat + sticky {one(sticky)} s is inside Dapr's crash range ({min(crash_dapr):.1f}-{max(crash_dapr):.1f})",
      min(crash_dapr) <= sticky <= max(crash_dapr) and says(f"the crash cost {one(sticky)} seconds, inside the one to five seconds Dapr took"))
history = log("runs/crash-history-probe.log")
sticky_waits = re.findall(r"WORKFLOW_TASK_TIMED_OUT\s+\+\s*([\d.]+) ms TIMEOUT_TYPE_SCHEDULE_TO_START", history)
claim(f"sticky wait 5.0 s in the history probe ({sticky_waits})",
      sticky_waits and all(4950 < float(w) < 5050 for w in sticky_waits) and says("sat there for 5.0 seconds"))

# --- deploy and late provider ----------------------------------------------------------------------------------------
deploy_probe = re.findall(r"^DEPLOY (\w+)\s+\S+ outcome=(\w+) charges=(\d+)", log("runs/deploy-probe.log"), re.M)
dapr_deploys = charges_of("dapr-deploy") + [int(c) for e, _, c in deploy_probe if e == "Dapr"]
temporal_deploys = charges_of("temporal-deploy") + [int(c) for e, _, c in deploy_probe if e == "Temporal"]
claim(f"deploy: Dapr 2 every run ({dapr_deploys})", always(dapr_deploys, 2))
claim(f"deploy: Temporal 1 every run ({temporal_deploys})", always(temporal_deploys, 1))
runs = min(len(dapr_deploys), len(temporal_deploys))
claim(f"deploy table quotes {runs} runs, at least six", runs >= 6 and f"Charges, in each of {runs} runs" in text)
before = re.search(r"^RESULT Temporal late-temporal-hung\s+Result\[outcome=(\w+), charges=(\d+), refunds=(\d+)",
                   log("runs/late-provider-before-reconcile.log"), re.M)
claim("first safe version with a late provider: cancelled, 1 charge, 0 refunds",
      before and before.groups() == ("cancelled", "1", "0")
      and row("Temporal, charge hangs and the provider processes it late")
      == ("first safe version: 1 charge, cancelled, nothing refunded", "1 charge, refunded, cancelled"))
safe_deploys = [int(c) for c in re.findall(r"^RESULT Dapr\s+safe-dapr-deploy\s+Result\[outcome=shipped, charges=(\d+)",
                                            log("runs/safe-deploy-probe.log") + log("test-results.log"), re.M)]
claim(f"safe saga, Dapr deploy: 1 charge in every run, at least six ({safe_deploys})",
      len(safe_deploys) >= 6 and always(safe_deploys, 1))
mutant = re.search(r"^RESULT Dapr\s+dapr-compensation-outage\s+Result\[outcome=(\w+), charges=(\d+), refunds=(\d+)",
                   log("runs/compensation-outage-mutant.log"), re.M)
claim("second safe version under a provider outage: failed, 1 charge, 0 refunds",
      mutant and mutant.groups() == ("FAILED", "1", "0")
      and row("Dapr, provider down for 6 s as the refund starts")
      == ("second safe version: 1 charge, failed, nothing refunded", "1 charge, refunded, cancelled")
      and says("ended cancelled and refunded on Dapr and on Temporal")
      and all(result(o) and result(o)[2] == "cancelled" for o in ("dapr-compensation-outage", "temporal-compensation-outage")))
late = re.search(r"^LATE outcome=(\w+) charges=(\d+) refunds=(\d+)", log("test-results.log"), re.M)
claim("late provider with the status check: cancelled, 1 charge, 1 refund", late and late.groups() == ("cancelled", "1", "1"))

drained = [int(c) for c in re.findall(r"^RESULT Dapr\s+dapr-drained-deploy\s+Result\[outcome=shipped, charges=(\d+)",
                                       log("runs/drained-deploy-probe.log") + log("test-results.log"), re.M)]
claim(f"drained Dapr deploy: 1 charge in each of {len(drained)} runs, at least six, quoted",
      len(drained) >= 6 and always(drained, 1) and says(f"the same deploy charged once in each of {len(drained)} runs"))

# --- figures and chart: the files the article shows ------------------------------------------------------------------
# These read the site repository next to this one; a clone of the companion repository alone skips them.
import json
SITE = ROOT.parent.parent
if (SITE / "public/blog/dapr-vs-temporal-workflow-chaos-chart-1.json").exists():
    chart = json.loads((SITE / "public/blog/dapr-vs-temporal-workflow-chaos-chart-1.json").read_text(encoding="utf-8"))
    bar_of = {b["label"]: b["value"] for b in chart["bars"]}
    for label, key in [("Dapr · no delay", ("Dapr", "none")), ("Temporal · no delay", ("Temporal", "none")),
                       ("Dapr · app link +5 ms", ("Dapr", "dapr-app")), ("Temporal · app link +5 ms", ("Temporal", "temporal-app")),
                       ("Dapr · DB link +5 ms", ("Dapr", "dapr-store")), ("Temporal · DB link +5 ms", ("Temporal", "temporal-store"))]:
        ms = main[key][0]
        shown = one(ms) if ms < 100 else f"{ms:.0f}"
        claim(f"chart bar '{label}' = {shown} ms, in the alt text too",
              f"{bar_of.get(label):g}" == f"{float(shown):g}" and f"{shown} ms" in article)
    dapr_db = main[("Dapr", "dapr-store")][0] - main[("Dapr", "none")][0]
    temporal_db = main[("Temporal", "temporal-store")][0] - main[("Temporal", "none")][0]
    claim(f"figure 3 caption: DB link costs Dapr {dapr_db:.0f} ms and Temporal {temporal_db:.0f}",
          says(f"it costs Dapr {dapr_db:.0f} ms per step and Temporal {temporal_db:.0f}."))
    figures_src = (SITE / "blog-drafts/diagrams/dapr-vs-temporal-workflow-chaos.figures.mjs").read_text(encoding="utf-8")
    claim(f"figure 2 and its alt say {runs} deploy runs",
          f"Same test, {runs} runs each" in figures_src and f"same test, {runs} runs each" in article)
    for fragment in ["Link cut for 300 ms after the money moved: Dapr charged twice, Temporal once.",
                     "start-to-close timer\\n5 s, kept by"]:
        claim(f"figure 1 says '{fragment[:40]}'", fragment in figures_src)

# --- code excerpts: every block is a verbatim slice of the file its first line names ----------------------------------
import html as _html
for path, body in re.findall(r'<pre><code class="language-\w+">// (\S+)\n(.*?)</code></pre>', article, re.S):
    claim(f"excerpt {path} matches the repository", _html.unescape(body).strip() in log(path))

print(f"{checked - len(failures)}/{checked} claims hold")
for failure in failures:
    print("FAIL", failure)
sys.exit(1 if failures else 0)
