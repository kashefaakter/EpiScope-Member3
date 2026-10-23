"""
EpiScope - AN-4 Disease Forecasting

Requirements:
    - Spark ML based forecasting
    - At least 2 diseases
    - 1-4 week forecasting horizons
    - Lagged disease features
    - Lagged weather covariates
    - District-level forecasts
    - Forecast output for AN-5 rolling-origin backtesting
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.ml.regression import RandomForestRegressor


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FEATURE_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "features"
    / "district_disease_epiweek_features"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "analytics"
    / "forecast"
)

FORECAST_PATH = (
    OUTPUT_DIR
    / "forecast_results"
)

SUMMARY_PATH = (
    OUTPUT_DIR
    / "forecast_summary.json"
)


# ============================================================
# CONFIGURATION
# ============================================================

DISEASES = [
    "Dengue",
    "Cholera/Diarrhoeal Disease",
    "Influenza/ILI",
    "Malaria",
]

HORIZONS = [1, 2, 3, 4]

NUM_TREES = 80
MAX_DEPTH = 8
MIN_INSTANCES_PER_NODE = 5
SEED = 4345

SHUFFLE_PARTITIONS = 16


# ============================================================
# SPARK
# ============================================================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("EpiScope-AN4-Forecasting")
        .master("local[*]")
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(SHUFFLE_PARTITIONS),
        )
        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "true",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# LOAD FEATURE MART
# ============================================================

def load_features(spark):

    if not FEATURE_PATH.exists():

        raise FileNotFoundError(
            f"Feature mart not found:\n"
            f"{FEATURE_PATH}"
        )

    df = (
        spark.read
        .parquet(
            str(FEATURE_PATH)
        )
    )

    required_columns = {
        "epi_week",
        "epi_week_start",
        "district_name",
        "disease",
        "case_count",
        "population_total",
        "incidence_per_100k",
    }

    missing = (
        required_columns
        - set(df.columns)
    )

    if missing:

        raise ValueError(
            "Feature mart is missing required columns:\n"
            + ", ".join(sorted(missing))
        )

    return df


# ============================================================
# FEATURE SELECTION
# ============================================================

def identify_feature_columns(df):

    excluded = {
        # Identifiers
        "district_name",
        "division_name",
        "district_geocode",
        "disease",

        # Time identifiers
        "epi_week_start",

        # Current target
        "case_count",

        # Current incidence is directly derived
        # from current case_count, so excluding it
        # avoids target leakage.
        "incidence_per_100k",

        # Current-week aggregate counts can leak information
        # about the target when forecasting future weeks.
        "unique_encounter_count",
        "lab_confirmed_count",
        "admitted_count",
    }

    candidates = []

    numeric_types = {
        "int",
        "bigint",
        "double",
        "float",
        "long",
        "integer",
        "decimal",
    }

    for field in df.schema.fields:

        name = field.name

        if name in excluded:
            continue

        data_type = field.dataType.simpleString()

        # Explicitly keep known numeric feature columns.
        known_numeric = {
            "epi_week",
            "calendar_year",
            "calendar_month",
            "quarter",
            "month_sin",
            "month_cos",
            "week_sin",
            "week_cos",
            "population_total",
            "population_density",
            "temperature_mean",
            "temperature_max_mean",
            "temperature_min_mean",
            "precipitation_sum_mm",
            "humidity_mean",
            "epi_year",
        }

        if name in known_numeric:

            candidates.append(name)
            continue

        # Keep lag/rolling/anomaly/weather features.
        prefixes = (
            "cases_lag_",
            "incidence_lag_",
            "cases_rolling_",
            "temperature_mean_lag_",
            "temperature_max_mean_lag_",
            "temperature_min_mean_lag_",
            "precipitation_sum_mm_lag_",
            "humidity_mean_lag_",
            "cases_change_",
            "cases_growth_",
            "cases_vs_",
            "cases_zscore_",
            "admission_rate",
            "lab_confirmation_rate",
        )

        if name.startswith(prefixes):

            if (
                data_type in numeric_types
                or "decimal" in data_type
            ):

                candidates.append(name)

    # Remove duplicates while preserving order.
    candidates = list(
        dict.fromkeys(candidates)
    )

    if not candidates:

        raise ValueError(
            "No forecasting feature columns were identified."
        )

    return candidates


# ============================================================
# CREATE SUPERVISED DATASET
# ============================================================

def create_supervised_dataset(
    df,
    disease,
    horizon,
):
    """
    For week t:

        features = information available at week t

        target = case_count at week t + horizon

    Example:

        horizon=1 -> next week
        horizon=2 -> two weeks ahead
        horizon=3 -> three weeks ahead
        horizon=4 -> four weeks ahead
    """

    disease_df = (
        df
        .filter(
            F.col("disease")
            == disease
        )
    )

    district_window = (
        Window
        .partitionBy(
            "district_name"
        )
        .orderBy(
            F.col("epi_week_start")
        )
    )

    supervised = (
        disease_df
        .withColumn(
            "target_case_count",
            F.lead(
                F.col("case_count"),
                horizon,
            ).over(
                district_window
            ),
        )
    )

    # We need the future target.
    supervised = (
        supervised
        .filter(
            F.col(
                "target_case_count"
            ).isNotNull()
        )
    )

    # Need at least one previous-week case feature.
    supervised = (
        supervised
        .filter(
            F.col(
                "cases_lag_1"
            ).isNotNull()
        )
    )

    return supervised


# ============================================================
# TRAIN MODEL
# ============================================================

def train_one_model(
    supervised_df,
    feature_columns,
    disease,
    horizon,
):

    print()
    print(
        f"Training: "
        f"{disease} | "
        f"{horizon}-week ahead"
    )

    prepared = supervised_df

    # Cast all features to double.
    for column in feature_columns:

        prepared = prepared.withColumn(
            column,
            F.col(column).cast("double"),
        )

    prepared = prepared.withColumn(
        "target_case_count",
        F.col(
            "target_case_count"
        ).cast("double"),
    )

    prepared = prepared.filter(
        F.col(
            "target_case_count"
        ).isNotNull()
    )

    training_count = prepared.count()

    if training_count < 100:

        raise RuntimeError(
            f"Not enough training rows for "
            f"{disease}, horizon={horizon}. "
            f"Rows={training_count}"
        )

    # --------------------------------------------------------
    # Imputation
    # --------------------------------------------------------

    imputed_columns = [
        f"{column}_imputed"
        for column in feature_columns
    ]

    imputer = Imputer(
        inputCols=feature_columns,
        outputCols=imputed_columns,
        strategy="median",
    )

    # --------------------------------------------------------
    # Assemble features
    # --------------------------------------------------------

    assembler = VectorAssembler(
        inputCols=imputed_columns,
        outputCol="features",
        handleInvalid="keep",
    )

    # --------------------------------------------------------
    # Random Forest
    # --------------------------------------------------------

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="target_case_count",
        predictionCol="prediction",
        numTrees=NUM_TREES,
        maxDepth=MAX_DEPTH,
        minInstancesPerNode=MIN_INSTANCES_PER_NODE,
        featureSubsetStrategy="sqrt",
        seed=SEED + horizon,
    )

    pipeline = Pipeline(
        stages=[
            imputer,
            assembler,
            rf,
        ]
    )

    fitted_model = pipeline.fit(
        prepared
    )

    return fitted_model, training_count


# ============================================================
# GENERATE FORECAST
# ============================================================

def generate_forecast(
    fitted_model,
    forecast_df,
    feature_columns,
    disease,
    horizon,
):

    prepared = forecast_df

    for column in feature_columns:

        prepared = prepared.withColumn(
            column,
            F.col(column).cast("double"),
        )

    predictions = (
        fitted_model
        .transform(prepared)
    )

    result = (
        predictions
        .select(
            "district_name",
            "division_name",
            "district_geocode",
            "disease",
            "epi_week",
            "epi_week_start",
            "case_count",
            "population_total",
            "incidence_per_100k",
            "prediction",
        )
        .withColumn(
            "forecast_horizon_weeks",
            F.lit(horizon),
        )
        .withColumn(
            "forecast_date",
            F.date_add(
                F.col(
                    "epi_week_start"
                ),
                horizon * 7,
            ),
        )
        .withColumn(
            "predicted_cases",
            F.greatest(
                F.lit(0.0),
                F.col("prediction"),
            ),
        )
        .drop(
            "prediction"
        )
        .withColumn(
            "forecast_origin",
            F.col(
                "epi_week_start"
            ),
        )
        .withColumn(
            "model",
            F.lit(
                "Spark RandomForest"
            ),
        )
        .withColumn(
            "model_version",
            F.lit(
                "AN4-RF-v1"
            ),
        )
        .withColumn(
            "model_seed",
            F.lit(
                SEED + horizon
            ),
        )
    )

    return result


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print(
        "EpiScope AN-4 Disease Forecasting"
    )
    print("=" * 70)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    spark = create_spark()

    try:

        # ====================================================
        # 1. LOAD FEATURE MART
        # ====================================================

        print()
        print(
            "Loading Gold feature mart..."
        )

        df = load_features(
            spark
        )

        total_rows = df.count()

        print(
            f"Feature rows : "
            f"{total_rows:,}"
        )

        # ====================================================
        # 2. SHOW DISEASES
        # ====================================================

        available_diseases = [
            row["disease"]
            for row in (
                df
                .select("disease")
                .distinct()
                .orderBy("disease")
                .collect()
            )
        ]

        print()
        print(
            "Available diseases:"
        )

        for disease in available_diseases:

            print(
                f"  - {disease}"
            )

        # ====================================================
        # 3. SELECT FEATURES
        # ====================================================

        feature_columns = (
            identify_feature_columns(
                df
            )
        )

        print()
        print(
            f"Forecast feature count: "
            f"{len(feature_columns)}"
        )

        for feature in feature_columns:

            print(
                f"  - {feature}"
            )

        # ====================================================
        # 4. LATEST WEEK
        # ====================================================

        latest_week = (
            df
            .select(
                F.max(
                    "epi_week_start"
                ).alias(
                    "latest_week"
                )
            )
            .collect()[0][
                "latest_week"
            ]
        )

        if latest_week is None:

            raise RuntimeError(
                "Could not determine latest feature week."
            )

        print()
        print(
            f"Latest feature week: "
            f"{latest_week}"
        )

        # ====================================================
        # 5. FORECAST ORIGIN
        # ====================================================

        forecast_origin_df = (
            df
            .filter(
                F.col(
                    "epi_week_start"
                )
                == F.lit(
                    latest_week
                )
            )
        )

        forecast_origin_count = (
            forecast_origin_df.count()
        )

        print(
            f"Forecast origin rows: "
            f"{forecast_origin_count:,}"
        )

        # ====================================================
        # 6. TRAIN + FORECAST
        # ====================================================

        model_results = []

        trained_models = 0

        for disease in DISEASES:

            if disease not in available_diseases:

                print()
                print(
                    f"WARNING: {disease} "
                    f"not found. Skipping."
                )

                continue

            print()
            print(
                "=" * 70
            )

            print(
                f"DISEASE: {disease}"
            )

            print(
                "=" * 70
            )

            for horizon in HORIZONS:

                # --------------------------------------------
                # Create supervised data
                # --------------------------------------------

                supervised = (
                    create_supervised_dataset(
                        df,
                        disease,
                        horizon,
                    )
                )

                # --------------------------------------------
                # Train
                # --------------------------------------------

                fitted_model, training_count = (
                    train_one_model(
                        supervised,
                        feature_columns,
                        disease,
                        horizon,
                    )
                )

                trained_models += 1

                # --------------------------------------------
                # Current/latest forecast origin
                # --------------------------------------------

                origin = (
                    forecast_origin_df
                    .filter(
                        F.col(
                            "disease"
                        )
                        == disease
                    )
                )

                origin_count = origin.count()

                if origin_count == 0:

                    print(
                        f"WARNING: No origin rows "
                        f"for {disease}."
                    )

                    continue

                forecast = generate_forecast(
                    fitted_model,
                    origin,
                    feature_columns,
                    disease,
                    horizon,
                )

                forecast = (
                    forecast
                    .withColumn(
                        "training_rows",
                        F.lit(
                            training_count
                        ),
                    )
                    .withColumn(
                        "forecast_generated_at",
                        F.current_timestamp(),
                    )
                )

                model_results.append(
                    forecast
                )

                print(
                    f"Completed: "
                    f"{disease} | "
                    f"horizon={horizon} | "
                    f"training rows="
                    f"{training_count:,} | "
                    f"forecast rows="
                    f"{origin_count:,}"
                )

        # ====================================================
        # 7. CHECK MODELS
        # ====================================================

        if not model_results:

            raise RuntimeError(
                "No forecast model completed successfully."
            )

        # ====================================================
        # 8. UNION RESULTS
        # ====================================================

        combined = model_results[0]

        for result in model_results[1:]:

            combined = combined.unionByName(
                result,
                allowMissingColumns=True,
            )

        # ====================================================
        # 9. CLEAN FORECAST VALUES
        # ====================================================

        combined = (
            combined
            .withColumn(
                "predicted_cases",
                F.round(
                    F.greatest(
                        F.lit(0.0),
                        F.col(
                            "predicted_cases"
                        ),
                    ),
                    4,
                ),
            )
        )

        # ====================================================
        # 10. WRITE PARQUET
        # ====================================================

        print()
        print(
            "Writing forecast output..."
        )

        (
            combined
            .repartition(
                "disease",
                "forecast_horizon_weeks",
            )
            .write
            .mode("overwrite")
            .partitionBy(
                "disease",
                "forecast_horizon_weeks",
            )
            .parquet(
                str(
                    FORECAST_PATH
                )
            )
        )

        # ====================================================
        # 11. SUMMARY COUNTS
        # ====================================================

        forecast_rows = combined.count()

        district_count = (
            combined
            .select(
                "district_name"
            )
            .distinct()
            .count()
        )

        disease_count = (
            combined
            .select(
                "disease"
            )
            .distinct()
            .count()
        )

        horizon_count = (
            combined
            .select(
                "forecast_horizon_weeks"
            )
            .distinct()
            .count()
        )

        # ====================================================
        # 12. SUMMARY BY DISEASE/HORIZON
        # ====================================================

        summary_rows = (
            combined
            .groupBy(
                "disease",
                "forecast_horizon_weeks",
            )
            .agg(
                F.count("*").alias(
                    "forecast_rows"
                ),
                F.avg(
                    "predicted_cases"
                ).alias(
                    "mean_predicted_cases"
                ),
                F.min(
                    "predicted_cases"
                ).alias(
                    "min_predicted_cases"
                ),
                F.max(
                    "predicted_cases"
                ).alias(
                    "max_predicted_cases"
                ),
            )
            .orderBy(
                "disease",
                "forecast_horizon_weeks",
            )
            .collect()
        )

        # ====================================================
        # 13. SAVE JSON SUMMARY
        # ====================================================

        forecast_diseases = [
            row["disease"]
            for row in (
                combined
                .select("disease")
                .distinct()
                .orderBy("disease")
                .collect()
            )
        ]

        summary = {
            "project": "EpiScope",
            "analysis": (
                "AN-4 Disease Forecasting"
            ),
            "model": (
                "Spark RandomForestRegressor"
            ),
            "model_version": "AN4-RF-v1",
            "seed": SEED,
            "num_trees": NUM_TREES,
            "max_depth": MAX_DEPTH,
            "min_instances_per_node": (
                MIN_INSTANCES_PER_NODE
            ),
            "diseases_requested": DISEASES,
            "diseases_forecast": (
                forecast_diseases
            ),
            "horizons_weeks": HORIZONS,
            "feature_count": len(
                feature_columns
            ),
            "feature_columns": (
                feature_columns
            ),
            "input_feature_rows": int(
                total_rows
            ),
            "forecast_rows": int(
                forecast_rows
            ),
            "districts": int(
                district_count
            ),
            "diseases": int(
                disease_count
            ),
            "forecast_horizons": int(
                horizon_count
            ),
            "trained_models": int(
                trained_models
            ),
            "latest_forecast_origin": (
                str(latest_week)
            ),
            "forecast_output": str(
                FORECAST_PATH
            ),
            "model_summary": [
                {
                    "disease": row[
                        "disease"
                    ],
                    "horizon_weeks": int(
                        row[
                            "forecast_horizon_weeks"
                        ]
                    ),
                    "forecast_rows": int(
                        row[
                            "forecast_rows"
                        ]
                    ),
                    "mean_predicted_cases": (
                        float(
                            row[
                                "mean_predicted_cases"
                            ]
                        )
                    ),
                    "min_predicted_cases": (
                        float(
                            row[
                                "min_predicted_cases"
                            ]
                        )
                    ),
                    "max_predicted_cases": (
                        float(
                            row[
                                "max_predicted_cases"
                            ]
                        )
                    ),
                }
                for row in summary_rows
            ],
        }

        with open(
            SUMMARY_PATH,
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                summary,
                file,
                indent=2,
                ensure_ascii=False,
            )

        # ====================================================
        # 14. FINAL REPORT
        # ====================================================

        print()
        print("=" * 70)
        print(
            "AN-4 FORECASTING COMPLETE"
        )
        print("=" * 70)

        print(
            f"Input feature rows      : "
            f"{total_rows:,}"
        )

        print(
            f"Forecast rows           : "
            f"{forecast_rows:,}"
        )

        print(
            f"Districts forecast      : "
            f"{district_count}"
        )

        print(
            f"Diseases forecast       : "
            f"{disease_count}"
        )

        print(
            f"Horizons                : "
            f"{HORIZONS}"
        )

        print(
            f"Trained models          : "
            f"{trained_models}"
        )

        print(
            f"Forecast origin         : "
            f"{latest_week}"
        )

        print(
            f"Feature count           : "
            f"{len(feature_columns)}"
        )

        print()
        print(
            "Disease / horizon summary:"
        )

        for row in summary_rows:

            print(
                f"  {row['disease']} | "
                f"{row['forecast_horizon_weeks']} week | "
                f"rows="
                f"{row['forecast_rows']:,} | "
                f"mean predicted="
                f"{row['mean_predicted_cases']:.2f}"
            )

        print()
        print(
            f"Forecast output : "
            f"{FORECAST_PATH}"
        )

        print(
            f"Summary         : "
            f"{SUMMARY_PATH}"
        )

        print("=" * 70)

    finally:

        spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()