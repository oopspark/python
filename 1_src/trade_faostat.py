# ======================================================
# Global Soybean Trade Network (2022)
# - Network summary
# - Top exporters/importers
# - Major importers auto-pick & HHI comparison
# - Korea IPEI scenarios (US/Brazil/Canada link removal)
# - (Optional) Global efficiency scenarios (node removal)
# ======================================================

import pandas as pd
import numpy as np
import networkx as nx


# ======================================================
# 0) Helpers
# ======================================================
def match_node(G, candidates, *, contains_ok=True):
    """Try to match a node name in G from candidate strings."""
    nodes = list(G.nodes())
    node_set = set(nodes)

    # exact match
    for c in candidates:
        if c in node_set:
            return c

    # contains match (case-insensitive)
    if contains_ok:
        for c in candidates:
            cl = c.lower()
            for n in nodes:
                if cl == n.lower():
                    return n
            for n in nodes:
                if cl in n.lower():
                    return n

    return None


def safe_remove_edge(G, u, v):
    if u is not None and v is not None and G.has_edge(u, v):
        G.remove_edge(u, v)


# ======================================================
# 1) Build network (CSV -> DiGraph)
# ======================================================
def build_trade_network(csv_path, src_col, dst_col, w_col, *, encoding="utf-8-sig"):
    df = pd.read_csv(csv_path, encoding=encoding)

    df = df.dropna(subset=[src_col, dst_col, w_col]).copy()
    df[w_col] = pd.to_numeric(df[w_col], errors="coerce")
    df = df.dropna(subset=[w_col])
    df = df[df[w_col] > 0]

    G = nx.DiGraph()
    for _, r in df.iterrows():
        u = r[src_col]
        v = r[dst_col]
        w = float(r[w_col])
        if G.has_edge(u, v):
            G[u][v]["weight"] += w
        else:
            G.add_edge(u, v, weight=w)

    return G, df


# ======================================================
# 2) Basic measures
# ======================================================
def compute_modularity_and_communities(G):
    UG = G.to_undirected()
    communities = nx.algorithms.community.louvain_communities(
        UG, weight="weight", seed=42
    )
    modularity = nx.algorithms.community.modularity(UG, communities, weight="weight")

    partition = {}
    for i, com in enumerate(communities):
        for n in com:
            partition[n] = i
    return partition, modularity


def summarize_network(G, year, modularity):
    N = G.number_of_nodes()
    M = G.number_of_edges()

    degrees = dict(G.degree())
    w_degrees = dict(G.degree(weight="weight"))

    avg_degree = float(np.mean(list(degrees.values()))) if degrees else 0.0
    avg_w_degree = float(np.mean(list(w_degrees.values()))) if w_degrees else 0.0

    density = nx.density(G)
    clustering = nx.average_clustering(G.to_undirected())

    UG = G.to_undirected()
    if UG.number_of_nodes() <= 1:
        L, Dia = np.nan, np.nan
    else:
        if nx.is_connected(UG):
            giant = UG
        else:
            giant = UG.subgraph(max(nx.connected_components(UG), key=len))
        L = nx.average_shortest_path_length(giant)
        Dia = nx.diameter(giant)

    return {
        "Year": year,
        "Nodes": N,
        "Edges": M,
        "Graph Density": round(density, 4),
        "Average Path Length (giant)": round(L, 4) if pd.notna(L) else np.nan,
        "Network Diameter (giant)": int(Dia) if pd.notna(Dia) else np.nan,
        "Average Degree": round(avg_degree, 4),
        "Average Weighted Degree": round(avg_w_degree, 4),
        "Average Clustering Coefficient": round(clustering, 4),
        "Modularity": round(modularity, 4),
    }


def topk_weighted_degree(G, kind="out", k=20, include=None):
    include = include or []
    if kind == "out":
        d = dict(G.out_degree(weight="weight"))
        col = "weighted_outdegree"
    else:
        d = dict(G.in_degree(weight="weight"))
        col = "weighted_indegree"

    df = (
        pd.DataFrame({"country": list(d.keys()), col: list(d.values())})
        .sort_values(col, ascending=False)
        .reset_index(drop=True)
    )

    exist = set(df["country"])
    extra = [c for c in include if c and c not in exist]
    if extra:
        df_extra = pd.DataFrame({"country": extra, col: [d.get(c, 0.0) for c in extra]})
        df = pd.concat([df, df_extra], ignore_index=True)

    return df.head(k).reset_index(drop=True)


# ======================================================
# 3) Inbound partner shares + HHI
# ======================================================
def inbound_partner_shares(G, target):
    preds = list(G.predecessors(target))
    rows = []
    total = 0.0
    for p in preds:
        w = float(G[p][target].get("weight", 0.0))
        total += w
        rows.append((p, w))

    df = pd.DataFrame(rows, columns=["partner", "import_value"])
    if total > 0:
        df["share"] = df["import_value"] / total
    else:
        df["share"] = np.nan

    df = df.sort_values("import_value", ascending=False).reset_index(drop=True)
    hhi = float(np.nansum(df["share"].values ** 2)) if total > 0 else np.nan
    return df, total, hhi


def hhi_summary_for_targets(G, targets, *, topn=5):
    """
    targets: node list
    returns:
      - df summary: total_import, HHI, top shares, top partners string
      - partner_details: dict[country] -> partner table
    """
    rows = []
    partner_details = {}

    for t in targets:
        if t is None or t not in G:
            continue

        df_partners, total, hhi = inbound_partner_shares(G, t)

        shares = df_partners["share"].values if total > 0 else np.array([])
        top1 = float(shares[0]) if len(shares) >= 1 else np.nan
        top2 = float(shares[:2].sum()) if len(shares) >= 2 else np.nan
        top3 = float(shares[:3].sum()) if len(shares) >= 3 else np.nan

        top_list = []
        for i in range(min(topn, len(df_partners))):
            p = df_partners.loc[i, "partner"]
            s = df_partners.loc[i, "share"]
            top_list.append(f"{p}({s:.1%})")
        top_partners_str = ", ".join(top_list)

        rows.append({
            "country": t,
            "total_import": total,
            "num_partners": int(df_partners.shape[0]),
            "HHI": hhi,
            "top1_share": top1,
            "top2_share": top2,
            "top3_share": top3,
            "top_partners": top_partners_str
        })

        partner_details[t] = df_partners

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("total_import", ascending=False).reset_index(drop=True)

    return df, partner_details


# ======================================================
# 4) IPEI (Korea) scenarios
#    IPEI = Σ_s (w_s/W) * (1 / d_sK), where s are baseline direct sources
# ======================================================
def compute_ipei_from_baseline_sources(benchmark_G, scenario_G, target):
    sources = list(benchmark_G.predecessors(target))
    weights = {s: float(benchmark_G[s][target].get("weight", 0.0)) for s in sources}
    W = sum(weights.values())
    if W <= 0:
        return np.nan

    score = 0.0
    for s, w in weights.items():
        if w <= 0:
            continue
        try:
            d = nx.shortest_path_length(scenario_G, s, target)
            if d > 0:
                score += (w / W) * (1.0 / d)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            pass

    return float(score)


def summarize_korea_ipei_scenarios(G, korea_node, us_node, brazil_node, canada_node, year):
    ipei0 = compute_ipei_from_baseline_sources(G, G, korea_node)

    scenarios = []

    def add(name, edges_to_remove):
        Gs = G.copy()
        for (u, v) in edges_to_remove:
            safe_remove_edge(Gs, u, v)

        ipei = compute_ipei_from_baseline_sources(G, Gs, korea_node)
        delta = (ipei / ipei0) if (pd.notna(ipei0) and ipei0 != 0 and pd.notna(ipei)) else np.nan
        scenarios.append({
            "Year": year,
            "Scenario": name,
            "IPEI_K": round(ipei, 4) if pd.notna(ipei) else np.nan,
            "Delta_IPEI": round(delta, 4) if pd.notna(delta) else np.nan,
        })

    add("Baseline", [])
    add("S1—US→Korea removed", [(us_node, korea_node)])
    add("S2—Brazil→Korea removed", [(brazil_node, korea_node)])
    add("S3—US & Brazil→Korea removed", [(us_node, korea_node), (brazil_node, korea_node)])
    add("S4—US & Brazil & Canada→Korea removed", [(us_node, korea_node), (brazil_node, korea_node), (canada_node, korea_node)])

    return pd.DataFrame(scenarios)


# ======================================================
# 5) (Optional) global efficiency scenarios
# ======================================================
def network_efficiency(G):
    UG = G.to_undirected()
    nodes = list(UG.nodes())
    n = len(nodes)
    if n <= 1:
        return 0.0

    eff = 0.0
    for i in range(n):
        for j in range(i + 1, n):
            try:
                d = nx.shortest_path_length(UG, nodes[i], nodes[j])
                if d > 0:
                    eff += 1 / d
            except nx.NetworkXNoPath:
                pass

    return eff * 2 / (n * (n - 1))


def summarize_global_eff_scenarios(benchmark_G, scenario_graphs, year):
    rows = []
    E0 = network_efficiency(benchmark_G)
    rows.append({"Year": year, "Scenario": "Benchmark", "Network Efficiency": round(E0, 4)})

    for name, Gs in scenario_graphs.items():
        E = network_efficiency(Gs)
        rows.append({"Year": year, "Scenario": name, "Network Efficiency": round(E, 4)})

    return pd.DataFrame(rows)


# ======================================================
# 6) MAIN (edit only this section)
# ======================================================
YEAR = 2022

# ---- 파일/컬럼 설정 (너 데이터에 맞게)
csv_path = "/home/user/문서/workspace/python/data/콩_무역_2022.csv"
SRC = "exporter"
DST = "importer"
W   = "value"

# ---- “주요 수입국” 자동 선정 기준
TOP_IMPORTERS_N = 10       # 가중 수입차수 상위 N개국
HHI_TOPN_PARTNERS = 5      # HHI 테이블에 보여줄 상위 파트너 수
PRINT_PARTNER_TABLE_TOP = 15

# ---- 옵션
RUN_GLOBAL_EFFICIENCY = True


def main():
    # 1) Build graph
    G, df_raw = build_trade_network(csv_path, SRC, DST, W)

    # 2) Summary
    partition, Q = compute_modularity_and_communities(G)
    df_network_summary = pd.DataFrame([summarize_network(G, year=YEAR, modularity=Q)])
    print("\n=== Network summary ===")
    print(df_network_summary)

    # 3) Key nodes (for forced include + Korea IPEI)
    korea = match_node(G, ["Korea, Rep.", "Republic of Korea", "Korea, Republic of", "Korea"])
    china = match_node(G, ["China, mainland", "China"])
    japan = match_node(G, ["Japan", "Japan, (Mainland)", "Japan (excluding the Ryukyu Islands)"])
    brazil = match_node(G, ["Brazil"])
    usa = match_node(G, ["United States of America", "USA", "United States"])
    canada = match_node(G, ["Canada"])

    print("\n[Matched nodes]")
    print("Korea :", korea)
    print("China :", china)
    print("Japan :", japan)
    print("Brazil:", brazil)
    print("USA   :", usa)
    print("Canada:", canada)

    if korea is None:
        raise ValueError("Korea node 매칭 실패: 데이터의 한국 국가명을 candidates 리스트에 추가해야 함.")

    # 4) Top exporters/importers
    df_top_exporters = topk_weighted_degree(G, kind="out", k=20, include=[korea, china, japan, brazil, usa])
    df_top_importers = topk_weighted_degree(G, kind="in",  k=20, include=[korea, china, japan, brazil, usa])

    print("\n=== Top exporters (weighted outdegree) ===")
    print(df_top_exporters)

    print("\n=== Top importers (weighted indegree) ===")
    print(df_top_importers)

    # 5) Major importers auto-pick + force-include (KOR/CHN/JPN)
    top_importer_nodes = list(df_top_importers["country"].head(TOP_IMPORTERS_N).values)
    forced = [korea, china, japan]
    # 중복 제거 + None 제거
    auto_targets = []
    for x in (top_importer_nodes + forced):
        if x is not None and x not in auto_targets:
            auto_targets.append(x)

    df_hhi_compare, partner_tables = hhi_summary_for_targets(
        G, auto_targets, topn=HHI_TOPN_PARTNERS
    )

    print(f"\n=== Import concentration (HHI) comparison: top {TOP_IMPORTERS_N} importers + (KOR/CHN/JPN) ===")
    print(df_hhi_compare)

    # 6) 상세 파트너 테이블(각 국가별 상위 15개)
    for c in auto_targets:
        if c in partner_tables:
            print(f"\n--- {c} inbound partners (top {PRINT_PARTNER_TABLE_TOP}) ---")
            print(partner_tables[c].head(PRINT_PARTNER_TABLE_TOP))

    # 7) Korea IPEI scenarios (필요한 노드가 없으면 자동으로 NaN 처리됨)
    df_korea_ipei = summarize_korea_ipei_scenarios(G, korea, usa, brazil, canada, year=YEAR)
    print("\n=== Korea IPEI scenarios ===")
    print(df_korea_ipei)

    # 8) (Optional) Global efficiency scenarios
    if RUN_GLOBAL_EFFICIENCY:
        scenario_graphs = {}
        scenario_def = {
            "Scenario 1—Brazil excluded": [brazil],
            "Scenario 2—US excluded": [usa],
            "Scenario 3—Brazil & US excluded": [brazil, usa],
        }
        for name, remove_nodes in scenario_def.items():
            Gs = G.copy()
            Gs.remove_nodes_from([n for n in remove_nodes if n is not None and n in Gs])
            scenario_graphs[name] = Gs

        df_global_eff = summarize_global_eff_scenarios(G, scenario_graphs, year=YEAR)
        print("\n=== Global efficiency scenarios (optional) ===")
        print(df_global_eff)

    # 9) (Optional) CSV 저장하고 싶으면 아래 주석 해제
    df_network_summary.to_csv("out_network_summary.csv", index=False)
    df_top_exporters.to_csv("out_top_exporters.csv", index=False)
    df_top_importers.to_csv("out_top_importers.csv", index=False)
    df_hhi_compare.to_csv("out_hhi_compare_major_importers.csv", index=False)
    df_korea_ipei.to_csv("out_korea_ipei_scenarios.csv", index=False)


if __name__ == "__main__":
    main()
