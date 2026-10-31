
"""
EpiScope AN-7
Resource Allocation Optimizer

Purpose
-------
Allocate a fixed national resource budget across Bangladesh
districts using Linear Programming.

Resources:
    1. Hospital beds
    2. Saline units
    3. Diagnostic test kits

Optimization objective:
    Minimize weighted unmet demand.

Constraints:
    - National resource budgets are fixed.
    - Allocation cannot be negative.
    - Allocation cannot exceed planning demand.
    - Optional district equity floor.
    - Population-proportional baseline is calculated separately.

Ground truth is NEVER used here.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

import pandas as pd
import pulp


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEMAND_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "resource_demand"
    / "forecast_resource_demand"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "resource_allocation"
)

ALLOCATION_OUTPUT = (
    OUTPUT_ROOT
    / "optimized_allocation.csv"
)

BASELINE_OUTPUT = (
    OUTPUT_ROOT
    / "population_proportional_baseline.csv"
)

COMPARISON_OUTPUT = (
    OUTPUT_ROOT
    / "allocation_comparison.csv"
)

SUMMARY_OUTPUT = (
    OUTPUT_ROOT
    / "allocation_summary.json"
)


# ============================================================
# OPTIMIZATION SETTINGS
# ============================================================

# Resource weights in the objective function.
#
# Higher weight means unmet demand for that resource receives
# higher priority.
BED_WEIGHT = 3.0
SALINE_WEIGHT = 1.0
TEST_KIT_WEIGHT = 2.0


# Equity floor.
#
# Enforce a population-proportional baseline share of the
# available national budget for each resource, not a hard
# percentage of each district's own demand.
#
# This preserves the intended formulation:
#   x_d,r >= alpha * p_d * sum(x_d',r)
# with alpha = EQUITY_FLOOR and p_d = district population share.
EQUITY_FLOOR = 0.15


# Resource budget fractions.
#
# These are derived from the national planning requirement.
# They intentionally create a constrained allocation problem.
#
# Example:
# 0.70 means only 70% of total planning requirement is
# available nationally.
BED_BUDGET_FRACTION = 0.70
SALINE_BUDGET_FRACTION = 0.70
TEST_KIT_BUDGET_FRACTION = 0.70


# ============================================================
# HELPERS
# ============================================================

def safe_float(value):
    if value is None:
        return 0.0

    try:
        value = float(value)

        if not math.isfinite(value):
            return 0.0

        return value

    except Exception:
        return 0.0


def ensure_output_directory():
    OUTPUT_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )


# ============================================================
# LOAD AN-6 OUTPUT
# ============================================================

def load_demand_data():
    if not DEMAND_PATH.exists():
        raise FileNotFoundError(
            "AN-6 demand output not found:\n"
            f"{DEMAND_PATH}\n\n"
            "Run first:\n"
            "python analytics\\forecast\\resource_demand.py"
        )

    print(
        f"Reading AN-6 demand output:\n{DEMAND_PATH}"
    )

    df = pd.read_parquet(
        DEMAND_PATH
    )

    required_columns = [
        "district_name",
        "disease",
        "forecast_horizon_week",
        "population_total",
        "forecast_cases",
        "bed_planning_requirement",
        "saline_planning_requirement",
        "test_kit_planning_requirement",
    ]

    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "AN-6 output is missing required columns:\n"
            + "\n".join(missing)
        )

    return df


# ============================================================
# SELECT PLANNING HORIZON
# ============================================================

def select_planning_horizon(df):
    """
    AN-7 uses the four-week planning horizon.

    We aggregate demand across diseases for each district.
    """

    available_horizons = sorted(
        df["forecast_horizon_week"]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )

    expected_horizons = [1, 2, 3, 4]
    if available_horizons != expected_horizons:
        raise ValueError(
            "The four-week planning window requires forecast horizons "
            f"{expected_horizons}; found {available_horizons}."
        )

    selected_horizon = 4

    print(
        f"Available horizons : {available_horizons}"
    )

    print(
        f"Planning horizon    : {selected_horizon} weeks"
    )

    return selected_horizon


# ============================================================
# AGGREGATE DISTRICT DEMAND
# ============================================================

def aggregate_district_demand(
    df,
    planning_horizon
):
    required_stock_columns = {
        "available_beds",
        "saline_stock",
        "test_kit_stock",
        "stock_data_status",
    }
    missing_stock_columns = required_stock_columns - set(df.columns)
    if missing_stock_columns:
        raise ValueError(
            "AN-6 output is missing stock-validation columns: "
            + ", ".join(sorted(missing_stock_columns))
        )

    selected = (
        df[
            df["forecast_horizon_week"].astype(int)
            .between(1, planning_horizon)
        ]
        .copy()
    )

    if selected.empty:
        raise ValueError(
            f"No data found for horizon "
            f"{planning_horizon}."
        )

    expected_horizons = list(range(1, planning_horizon + 1))
    actual_horizons = sorted(
        selected["forecast_horizon_week"].astype(int).unique().tolist()
    )
    if actual_horizons != expected_horizons:
        raise ValueError(
            "Four-week demand requires each forecast horizon exactly once; "
            f"expected {expected_horizons}, found {actual_horizons}."
        )
    if selected.duplicated(
        ["district_name", "disease", "forecast_horizon_week"]
    ).any():
        raise ValueError(
            "AN-6 has duplicate district/disease/horizon demand rows."
        )
    horizon_counts = selected.groupby(
        ["district_name", "disease"]
    )["forecast_horizon_week"].nunique()
    expected_group_count = (
        selected["district_name"].nunique()
        * selected["disease"].nunique()
    )
    if (
        len(horizon_counts) != expected_group_count
        or horizon_counts.ne(planning_horizon).any()
    ):
        raise ValueError(
            "Every district/disease pair must contain all four forecast "
            "horizons before aggregation."
        )

    # District population and physical stock are repeated across disease
    # horizons; retain each district's one-snapshot values only once.
    population = (
        selected
        .groupby("district_name", as_index=False)
        ["population_total"]
        .max()
    )
    stock = (
        selected
        .groupby("district_name", as_index=False)
        .agg(
            available_beds=("available_beds", "max"),
            saline_stock=("saline_stock", "max"),
            test_kit_stock=("test_kit_stock", "max"),
            stock_data_status=("stock_data_status", "first"),
        )
    )
    if stock["stock_data_status"].ne("AVAILABLE").any():
        unavailable = stock.loc[
            stock["stock_data_status"].ne("AVAILABLE"), "district_name"
        ].tolist()
        raise ValueError(
            "Cannot optimize with missing or incomplete S6 stock for: "
            + ", ".join(unavailable)
        )
    if stock[["available_beds", "saline_stock", "test_kit_stock"]].isna().any().any():
        raise ValueError("S6 national stock totals contain missing values.")

    demand = (
        selected
        .groupby("district_name", as_index=False)
        .agg(
            forecast_cases=(
                "forecast_cases",
                "sum"
            ),
            bed_demand=(
                "bed_planning_requirement",
                "sum"
            ),
            saline_demand=(
                "saline_planning_requirement",
                "sum"
            ),
            test_kit_demand=(
                "test_kit_planning_requirement",
                "sum"
            ),
        )
    )

    district = demand.merge(
        population,
        on="district_name",
        how="left"
    )
    district = district.merge(
        stock,
        on="district_name",
        how="left",
        validate="one_to_one",
    )

    district["population_total"] = (
        district["population_total"]
        .fillna(0)
        .astype(float)
    )

    for column in [
        "forecast_cases",
        "bed_demand",
        "saline_demand",
        "test_kit_demand",
    ]:
        district[column] = (
            district[column]
            .fillna(0)
            .astype(float)
            .clip(lower=0)
        )

    district = district.sort_values(
        "district_name"
    ).reset_index(drop=True)

    if len(district) != 64:
        raise ValueError(
            "AN-7 requires all 64 Bangladesh districts; "
            f"found {len(district)}."
        )

    return district


# ============================================================
# NATIONAL BUDGETS
# ============================================================

def calculate_budgets(district):
    total_beds = district[
        "bed_demand"
    ].sum()

    total_saline = district[
        "saline_demand"
    ].sum()

    total_test_kits = district[
        "test_kit_demand"
    ].sum()

    stock_available = {
        "beds": safe_float(district["available_beds"].sum()),
        "saline": safe_float(district["saline_stock"].sum()),
        "test_kits": safe_float(district["test_kit_stock"].sum()),
    }
    budgets = {
        "beds": min(
            total_beds * BED_BUDGET_FRACTION,
            stock_available["beds"],
        ),
        "saline": min(
            total_saline * SALINE_BUDGET_FRACTION,
            stock_available["saline"],
        ),
        "test_kits": min(
            total_test_kits * TEST_KIT_BUDGET_FRACTION,
            stock_available["test_kits"],
        ),
    }
    district.attrs["national_stock_available"] = stock_available

    print()
    print("National planning demand:")
    print(
        f"  Beds       : {total_beds:,.0f}"
    )
    print(
        f"  Saline     : {total_saline:,.0f}"
    )
    print(
        f"  Test kits  : {total_test_kits:,.0f}"
    )

    print()
    print("Fixed national budgets:")
    print(
        f"  Beds       : {budgets['beds']:,.0f}"
    )
    print(
        f"  Saline     : {budgets['saline']:,.0f}"
    )
    print(
        f"  Test kits  : {budgets['test_kits']:,.0f}"
    )
    print(
        "Budgets are capped by the selected S6 snapshot stock."
    )

    return budgets


# ============================================================
# POPULATION-PROPORTIONAL BASELINE
# ============================================================

def build_population_baseline(
    district,
    budgets
):
    baseline = district.copy()

    total_population = (
        baseline["population_total"]
        .sum()
    )

    if total_population <= 0:
        raise ValueError(
            "Total population is zero."
        )

    population_share = (
        baseline["population_total"]
        / total_population
    )

    baseline["population_share"] = (
        population_share
    )

    for resource, demand_column, allocation_column in [
        ("beds", "bed_demand", "baseline_beds"),
        ("saline", "saline_demand", "baseline_saline"),
        ("test_kits", "test_kit_demand", "baseline_test_kits"),
    ]:
        baseline[allocation_column] = population_proportional_allocation(
            demand=baseline[demand_column].to_numpy(dtype=float),
            population_share=population_share.to_numpy(dtype=float),
            budget=budgets[resource],
        )

    # Baseline unmet demand.
    baseline["baseline_bed_unmet"] = (
        baseline["bed_demand"]
        - baseline["baseline_beds"]
    ).clip(lower=0)

    baseline["baseline_saline_unmet"] = (
        baseline["saline_demand"]
        - baseline["baseline_saline"]
    ).clip(lower=0)

    baseline["baseline_test_kit_unmet"] = (
        baseline["test_kit_demand"]
        - baseline["baseline_test_kits"]
    ).clip(lower=0)

    baseline["baseline_total_unmet"] = (
        baseline["baseline_bed_unmet"]
        + baseline["baseline_saline_unmet"]
        + baseline["baseline_test_kit_unmet"]
    )

    return baseline


def population_proportional_allocation(demand, population_share, budget):
    """Allocate by population share, capping each district at eligible demand."""
    allocation = [0.0] * len(demand)
    remaining = min(float(budget), float(sum(demand)))
    eligible = {
        index
        for index, value in enumerate(demand)
        if float(value) > 0 and float(population_share[index]) > 0
    }

    while remaining > 1e-9 and eligible:
        weight_total = sum(float(population_share[i]) for i in eligible)
        if weight_total <= 0:
            break
        proposed = {
            index: remaining * float(population_share[index]) / weight_total
            for index in eligible
        }
        allocated_now = 0.0
        capped = set()
        for index, amount in proposed.items():
            room = max(0.0, float(demand[index]) - allocation[index])
            increment = min(room, amount)
            allocation[index] += increment
            allocated_now += increment
            if room - increment <= 1e-9:
                capped.add(index)
        remaining -= allocated_now
        eligible -= capped
        if allocated_now <= 1e-9:
            break

    return allocation


# ============================================================
# GENERIC RESOURCE OPTIMIZER
# ============================================================

def optimize_resource(
    district,
    resource_name,
    demand_column,
    budget,
    weight
):
    """
    Solve:

        minimize sum(weight * unmet_i)

    subject to:

        sum(allocation_i) <= national budget

        allocation_i <= demand_i

        allocation_i >= alpha * p_i * sum(allocation_i)

        unmet_i = demand_i - allocation_i
    """

    districts = district[
        "district_name"
    ].tolist()

    demand = {
        row["district_name"]:
            max(
                0.0,
                safe_float(row[demand_column])
            )
        for _, row in district.iterrows()
    }

    problem = pulp.LpProblem(
        f"EpiScope_{resource_name}_Allocation",
        pulp.LpMinimize
    )

    allocation = problem.add_variable_dicts(
        f"alloc_{resource_name}",
        districts,
        lowBound=0,
        cat="Continuous",
    )

    unmet = problem.add_variable_dicts(
        f"unmet_{resource_name}",
        districts,
        lowBound=0,
        cat="Continuous",
    )

    # --------------------------------------------------------
    # Objective
    # --------------------------------------------------------

    problem += pulp.lpSum(
        weight * unmet[d]
        for d in districts
    )

    # --------------------------------------------------------
    # National budget
    # --------------------------------------------------------

    problem += (
        pulp.lpSum(
            allocation[d]
            for d in districts
        )
        <= budget
    )

    # --------------------------------------------------------
    # District constraints
    # --------------------------------------------------------

    total_demand = sum(demand.values())
    if total_demand <= 0:
        total_demand = 1.0

    for d in districts:
        district_population = safe_float(
            district.loc[district["district_name"] == d, "population_total"].iloc[0]
            if not district[district["district_name"] == d].empty
            else 0.0
        )
        total_population = safe_float(
            district["population_total"].sum()
        )
        if total_population <= 0:
            raise ValueError("Total population is zero.")
        population_share = (
            district_population / total_population
            if total_population > 0
            else 0.0
        )

        # Cannot allocate above demand.
        problem += (
            allocation[d]
            <= demand[d]
        )

        # Equity floor: each district receives at least its population share
        # of the total optimized allocation for this resource.
        problem += (
            allocation[d]
            >= EQUITY_FLOOR * population_share * pulp.lpSum(
                allocation[k]
                for k in districts
            )
        )

        # Demand balance is exact so the reported unmet value cannot drift.
        problem += (
            unmet[d] == demand[d] - allocation[d]
        )

    # --------------------------------------------------------
    # Solve
    # --------------------------------------------------------

    solver = pulp.PULP_CBC_CMD(msg=False)
    problem.solve(solver)

    if problem.status != pulp.LpStatusOptimal:
        raise RuntimeError(
            f"{resource_name} optimization failed. "
            f"Solver status: {pulp.LpStatus.get(problem.status, problem.status)}"
        )

    allocations = []

    for d in districts:

        allocated = safe_float(
            pulp.value(
                allocation[d]
            )
        )

        unmet_value = safe_float(
            pulp.value(
                unmet[d]
            )
        )

        allocations.append(
            {
                "district_name": d,
                f"optimized_{resource_name}":
                    allocated,
                f"optimized_{resource_name}_unmet":
                    unmet_value,
            }
        )

    allocation_total = sum(
        row[f"optimized_{resource_name}"] for row in allocations
    )
    tolerance = 1e-6
    for row in allocations:
        allocated = row[f"optimized_{resource_name}"]
        unmet_value = row[f"optimized_{resource_name}_unmet"]
        eligible_demand = demand[row["district_name"]]
        if allocated < -tolerance or allocated > eligible_demand + tolerance:
            raise RuntimeError(
                f"{resource_name} allocation violates nonnegative/demand bounds "
                f"for {row['district_name']}."
            )
        if abs(unmet_value - (eligible_demand - allocated)) > tolerance:
            raise RuntimeError(
                f"{resource_name} unmet demand does not equal demand minus "
                f"allocation for {row['district_name']}."
            )
    if allocation_total > budget + tolerance:
        raise RuntimeError(
            f"{resource_name} total allocation exceeds its national budget."
        )

    return pd.DataFrame(
        allocations
    )


# ============================================================
# RUN ALL OPTIMIZERS
# ============================================================

def optimize_all_resources(
    district,
    budgets
):
    print()
    print("Running Linear Programming optimizer...")

    optimized = district[
        ["district_name"]
    ].copy()

    # --------------------------------------------------------
    # Beds
    # --------------------------------------------------------

    print(
        "  [1/3] Optimizing beds..."
    )

    beds = optimize_resource(
        district=district,
        resource_name="beds",
        demand_column="bed_demand",
        budget=budgets["beds"],
        weight=BED_WEIGHT,
    )

    optimized = optimized.merge(
        beds,
        on="district_name",
        how="left"
    )

    # --------------------------------------------------------
    # Saline
    # --------------------------------------------------------

    print(
        "  [2/3] Optimizing saline..."
    )

    saline = optimize_resource(
        district=district,
        resource_name="saline",
        demand_column="saline_demand",
        budget=budgets["saline"],
        weight=SALINE_WEIGHT,
    )

    optimized = optimized.merge(
        saline,
        on="district_name",
        how="left"
    )

    # --------------------------------------------------------
    # Test kits
    # --------------------------------------------------------

    print(
        "  [3/3] Optimizing test kits..."
    )

    test_kits = optimize_resource(
        district=district,
        resource_name="test_kits",
        demand_column="test_kit_demand",
        budget=budgets["test_kits"],
        weight=TEST_KIT_WEIGHT,
    )

    optimized = optimized.merge(
        test_kits,
        on="district_name",
        how="left"
    )

    return optimized


# ============================================================
# COMPARISON
# ============================================================

def build_comparison(
    district,
    optimized,
    baseline
):
    comparison = district[
        [
            "district_name",
            "population_total",
            "forecast_cases",
            "bed_demand",
            "saline_demand",
            "test_kit_demand",
        ]
    ].copy()

    comparison = comparison.merge(
        optimized,
        on="district_name",
        how="left"
    )

    comparison = comparison.merge(
        baseline[
            [
                "district_name",
                "population_share",
                "baseline_beds",
                "baseline_saline",
                "baseline_test_kits",
                "baseline_bed_unmet",
                "baseline_saline_unmet",
                "baseline_test_kit_unmet",
                "baseline_total_unmet",
            ]
        ],
        on="district_name",
        how="left"
    )

    comparison["optimized_total_unmet"] = (
        comparison["optimized_beds_unmet"]
        + comparison["optimized_saline_unmet"]
        + comparison["optimized_test_kits_unmet"]
    )

    comparison["unmet_reduction"] = (
        comparison["baseline_total_unmet"]
        - comparison["optimized_total_unmet"]
    )

    comparison["optimized_bed_coverage"] = (
        comparison["optimized_beds"]
        / comparison["bed_demand"].replace(
            0,
            1
        )
    ).clip(
        lower=0,
        upper=1
    )

    comparison["baseline_bed_coverage"] = (
        comparison["baseline_beds"]
        / comparison["bed_demand"].replace(
            0,
            1
        )
    ).clip(
        lower=0,
        upper=1
    )

    comparison["optimized_saline_coverage"] = (
        comparison["optimized_saline"]
        / comparison["saline_demand"].replace(
            0,
            1
        )
    ).clip(
        lower=0,
        upper=1
    )

    comparison["baseline_saline_coverage"] = (
        comparison["baseline_saline"]
        / comparison["saline_demand"].replace(
            0,
            1
        )
    ).clip(
        lower=0,
        upper=1
    )

    comparison["optimized_test_kit_coverage"] = (
        comparison["optimized_test_kits"]
        / comparison["test_kit_demand"].replace(
            0,
            1
        )
    ).clip(
        lower=0,
        upper=1
    )

    comparison["baseline_test_kit_coverage"] = (
        comparison["baseline_test_kits"]
        / comparison["test_kit_demand"].replace(
            0,
            1
        )
    ).clip(
        lower=0,
        upper=1
    )

    return comparison


# ============================================================
# SUMMARY
# ============================================================

def build_summary(
    district,
    budgets,
    comparison,
    planning_horizon
):
    national_stock_available = {
        "beds": safe_float(district["available_beds"].sum()),
        "saline": safe_float(district["saline_stock"].sum()),
        "test_kits": safe_float(district["test_kit_stock"].sum()),
    }
    constraint_checks = {}
    allocation_totals = {}
    for resource, demand_column, allocation_column, unmet_column in [
        ("beds", "bed_demand", "optimized_beds", "optimized_beds_unmet"),
        ("saline", "saline_demand", "optimized_saline", "optimized_saline_unmet"),
        ("test_kits", "test_kit_demand", "optimized_test_kits", "optimized_test_kits_unmet"),
    ]:
        allocations = comparison[allocation_column]
        demands = comparison[demand_column]
        unmet = comparison[unmet_column]
        allocated_total = safe_float(allocations.sum())
        allocation_totals[resource] = allocated_total
        constraint_checks[resource] = {
            "budget_respected": allocated_total <= budgets[resource] + 1e-6,
            "stock_respected": allocated_total <= national_stock_available[resource] + 1e-6,
            "nonnegative": bool((allocations >= -1e-6).all()),
            "not_above_demand": bool((allocations <= demands + 1e-6).all()),
            "unmet_matches_demand_minus_allocation": bool(
                ((unmet - (demands - allocations)).abs() <= 1e-6).all()
            ),
            "equity_floor_respected": bool(
                (
                    allocations
                    >= EQUITY_FLOOR
                    * comparison["population_share"]
                    * allocated_total
                    - 1e-6
                ).all()
            ),
        }
        if not all(constraint_checks[resource].values()):
            raise RuntimeError(
                f"AN-7 {resource} allocation failed one or more constraint checks: "
                f"{constraint_checks[resource]}"
            )

    summary = {
        "dataset": (
            "EpiScope AN-7 Resource Allocation"
        ),

        "generated_at": (
            datetime.utcnow().isoformat()
            + "Z"
        ),

        "planning_horizon_weeks": (
            int(planning_horizon)
        ),

        "district_count": (
            int(len(district))
        ),

        "demand_scope": (
            "Cumulative planning requirement across forecast horizons 1 through "
            f"{planning_horizon}."
        ),

        "national_budgets": {
            "beds": safe_float(
                budgets["beds"]
            ),
            "saline": safe_float(
                budgets["saline"]
            ),
            "test_kits": safe_float(
                budgets["test_kits"]
            ),
        },

        "national_stock_available": national_stock_available,
        "allocated": allocation_totals,
        "constraint_checks": constraint_checks,

        "optimizer": {
            "method": (
                "Linear Programming using PuLP's bundled CBC solver"
            ),
            "objective": (
                "Minimize weighted unmet demand"
            ),
            "bed_weight": BED_WEIGHT,
            "saline_weight": SALINE_WEIGHT,
            "test_kit_weight": TEST_KIT_WEIGHT,
            "equity_floor": EQUITY_FLOOR,
        },

        "budget_fractions": {
            "beds": BED_BUDGET_FRACTION,
            "saline": SALINE_BUDGET_FRACTION,
            "test_kits": TEST_KIT_BUDGET_FRACTION,
        },

        "optimized": {
            "bed_unmet": safe_float(
                comparison[
                    "optimized_beds_unmet"
                ].sum()
            ),

            "saline_unmet": safe_float(
                comparison[
                    "optimized_saline_unmet"
                ].sum()
            ),

            "test_kit_unmet": safe_float(
                comparison[
                    "optimized_test_kits_unmet"
                ].sum()
            ),

            "total_unmet": safe_float(
                comparison[
                    "optimized_total_unmet"
                ].sum()
            ),
        },

        "population_proportional_baseline": {
            "bed_unmet": safe_float(
                comparison[
                    "baseline_bed_unmet"
                ].sum()
            ),

            "saline_unmet": safe_float(
                comparison[
                    "baseline_saline_unmet"
                ].sum()
            ),

            "test_kit_unmet": safe_float(
                comparison[
                    "baseline_test_kit_unmet"
                ].sum()
            ),

            "total_unmet": safe_float(
                comparison[
                    "baseline_total_unmet"
                ].sum()
            ),
        },

        "unmet_demand_reduction": {
            "absolute": safe_float(
                comparison[
                    "unmet_reduction"
                ].sum()
            ),
        },

        "ground_truth_used": False,

        "outputs": {
            "optimized": str(
                ALLOCATION_OUTPUT
            ),
            "baseline": str(
                BASELINE_OUTPUT
            ),
            "comparison": str(
                COMPARISON_OUTPUT
            ),
        },
    }

    baseline_total = summary[
        "population_proportional_baseline"
    ]["total_unmet"]

    optimized_total = summary[
        "optimized"
    ]["total_unmet"]

    if baseline_total > 0:
        summary[
            "unmet_demand_reduction"
        ]["percentage"] = (
            (
                baseline_total
                - optimized_total
            )
            / baseline_total
            * 100.0
        )
    else:
        summary[
            "unmet_demand_reduction"
        ]["percentage"] = 0.0

    return summary


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 72)
    print("EpiScope AN-7 Resource Allocation Optimizer")
    print("=" * 72)

    ensure_output_directory()

    # --------------------------------------------------------
    # Load
    # --------------------------------------------------------

    print()
    print("[1/7] Loading AN-6 resource demand...")

    df = load_demand_data()

    print(
        f"Input rows          : {len(df):,}"
    )

    # --------------------------------------------------------
    # Planning horizon
    # --------------------------------------------------------

    print()
    print("[2/7] Selecting planning horizon...")

    planning_horizon = select_planning_horizon(
        df
    )

    # --------------------------------------------------------
    # District aggregation
    # --------------------------------------------------------

    print()
    print("[3/7] Aggregating district demand...")

    district = aggregate_district_demand(
        df,
        planning_horizon
    )

    print(
        f"Districts           : {len(district)}"
    )

    # --------------------------------------------------------
    # Budgets
    # --------------------------------------------------------

    print()
    print("[4/7] Creating fixed national budgets...")

    budgets = calculate_budgets(
        district
    )

    # --------------------------------------------------------
    # Baseline
    # --------------------------------------------------------

    print()
    print("[5/7] Building population-proportional baseline...")

    baseline = build_population_baseline(
        district,
        budgets
    )

    # --------------------------------------------------------
    # Optimize
    # --------------------------------------------------------

    print()
    print("[6/7] Solving optimization problem...")

    optimized = optimize_all_resources(
        district,
        budgets
    )

    # --------------------------------------------------------
    # Comparison
    # --------------------------------------------------------

    print()
    print("[7/7] Comparing optimized vs baseline...")

    comparison = build_comparison(
        district,
        optimized,
        baseline
    )

    # --------------------------------------------------------
    # Write files
    # --------------------------------------------------------

    optimized.to_csv(
        ALLOCATION_OUTPUT,
        index=False
    )

    baseline.to_csv(
        BASELINE_OUTPUT,
        index=False
    )

    comparison.to_csv(
        COMPARISON_OUTPUT,
        index=False
    )

    summary = build_summary(
        district=district,
        budgets=budgets,
        comparison=comparison,
        planning_horizon=planning_horizon,
    )

    with open(
        SUMMARY_OUTPUT,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            summary,
            f,
            indent=2,
            ensure_ascii=False
        )

    # --------------------------------------------------------
    # Console report
    # --------------------------------------------------------

    print()
    print("=" * 72)
    print("AN-7 RESOURCE ALLOCATION COMPLETE")
    print("=" * 72)

    print()
    print(
        f"Planning horizon     : "
        f"{planning_horizon} weeks"
    )

    print(
        f"Districts            : "
        f"{len(district)}"
    )

    print()
    print("Fixed national budgets:")

    print(
        f"  Beds               : "
        f"{budgets['beds']:,.0f}"
    )

    print(
        f"  Saline             : "
        f"{budgets['saline']:,.0f}"
    )

    print(
        f"  Test kits          : "
        f"{budgets['test_kits']:,.0f}"
    )

    print()
    print("Unmet demand:")
    print(
        f"  Optimized          : "
        f"{summary['optimized']['total_unmet']:,.0f}"
    )

    print(
        f"  Population baseline: "
        f"{summary['population_proportional_baseline']['total_unmet']:,.0f}"
    )

    print()
    print(
        "Unmet-demand reduction vs baseline:"
    )

    print(
        f"  Absolute           : "
        f"{summary['unmet_demand_reduction']['absolute']:,.0f}"
    )

    print(
        f"  Percentage         : "
        f"{summary['unmet_demand_reduction']['percentage']:.2f}%"
    )
    print()
    print("Constraint checks:")
    for resource, checks in summary["constraint_checks"].items():
        print(f"  {resource}: {checks}")

    print()
    print("Outputs:")

    print(
        f"  Optimized          : "
        f"{ALLOCATION_OUTPUT}"
    )

    print(
        f"  Baseline           : "
        f"{BASELINE_OUTPUT}"
    )

    print(
        f"  Comparison         : "
        f"{COMPARISON_OUTPUT}"
    )

    print(
        f"  Summary            : "
        f"{SUMMARY_OUTPUT}"
    )

    print()
    print(
        "Ground truth used    : NO"
    )


if __name__ == "__main__":
    main()
