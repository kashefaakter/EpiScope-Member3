"""
EpiScope AN-5
Rolling-origin forecasting backtest.

Design goals:
- Spark-based feature preparation and evaluation
- 52 weekly rolling origins
- 1-4 week forecasting horizons
- Spark ML Linear Regression model
- Seasonal-naive baseline
- 80% prediction intervals
- MAE
- 80% PI coverage
- Memory-safe for a 16 GB laptop

The AN-4 forecasting model can remain RandomForest-based.
For AN-5, Linear Regression is intentionally used because the backtest
requires many repeated model fits and must remain runnable on a laptop.
"""

import os
import json
import gc
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression


# ============================================================
# CONFIGURATION
# ============================================================

FEATURE_PATH = "data/gold/features/district_disease_epiweek_features"

OUTPUT_ROOT = "data/gold/analytics/forecast/backtest"

RESULT_PATH = os.path.join(
    OUTPUT_ROOT,
    "rolling_origin_results"
)

METRIC_PATH = os.path.join(
    OUTPUT_ROOT,
    "backtest_metrics"
)

SUMMARY_PATH = os.path.join(
    OUTPUT_ROOT,
    "backtest_summary.json"
)

HORIZONS = [1, 2, 3, 4]

BACKTEST_ORIGINS = 52

PI_LEVEL = 0.80

# Keep the model intentionally small.
MAX_ITER = 40
REG_PARAM = 0.05
ELASTIC_NET_PARAM = 0.20

# Only a compact, defensible subset of features is used.
# This is enough for:
# - autoregressive signal
# - seasonality
# - lagged weather
# - current epidemiological state
MODEL_FEATURES = [
    "cases_lag_1",
    "cases_lag_2",
    "cases_lag_4",
    "cases_lag_8",
    "cases_lag_12",

    "incidence_lag_1",
    "incidence_lag_2",
    "incidence_lag_4",

    "cases_rolling_mean_4w",
    "cases_rolling_mean_8w",

    "temperature_mean_lag_1",
    "temperature_mean_lag_2",
    "temperature_mean_lag_4",

    "precipitation_sum_mm_lag_1",
    "precipitation_sum_mm_lag_2",
    "precipitation_sum_mm_lag_4",

    "humidity_mean_lag_1",
    "humidity_mean_lag_2",
    "humidity_mean_lag_4",

    "month_sin",
    "month_cos",
    "week_sin",
    "week_cos",

    "cases_growth_rate_1w",
]


# ============================================================
# SPARK
# ============================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("EpiScope-AN5-RollingOrigin-Backtest")
        .master("local[*]")
        .config("spark.driver.memory", "5g")
        .config("spark.executor.memory", "5g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "16MB")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# UTILITIES
# ============================================================

def ensure_output_dirs():
    os.makedirs(OUTPUT_ROOT, exist_ok=True)


def safe_float(value):
    if value is None:
        return None

    try:
        value = float(value)

        if value != value:
            return None

        return value

    except Exception:
        return None


def percentile(values, q):
    """
    Simple percentile implementation on driver.

    Residual counts are small enough after aggregation and
    only one percentile vector is collected at a time.
    """

    clean = [
        float(v)
        for v in values
        if v is not None
    ]

    if not clean:
        return 0.0

    clean.sort()

    if len(clean) == 1:
        return clean[0]

    position = q * (len(clean) - 1)

    lower = int(position)
    upper = min(lower + 1, len(clean) - 1)

    fraction = position - lower

    return (
        clean[lower]
        + fraction * (clean[upper] - clean[lower])
    )


def collect_quantile(df, column, q):
    values = (
        df.select(column)
        .where(F.col(column).isNotNull())
        .limit(200000)
        .rdd
        .map(lambda r: float(r[0]))
        .collect()
    )

    return percentile(values, q)


# ============================================================
# LOAD FEATURE MART
# ============================================================

def load_feature_mart(spark):

    print()
    print("Loading Gold feature mart...")

    df = spark.read.parquet(FEATURE_PATH)

    required = [
        "district_name",
        "disease",
        "epi_week_start",
        "epi_week",
        "case_count",
        "population_total",
    ]

    missing = [
        c for c in required
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            "Feature mart is missing required columns: "
            + ", ".join(missing)
        )

    available_model_features = [
        c for c in MODEL_FEATURES
        if c in df.columns
    ]

    missing_model_features = [
        c for c in MODEL_FEATURES
        if c not in df.columns
    ]

    if missing_model_features:
        print(
            "WARNING: Missing optional model features:"
        )

        for c in missing_model_features:
            print("  -", c)

    print("Feature rows :", df.count())
    print(
        "Model features:",
        len(available_model_features)
    )

    latest = (
        df.select(
            F.max("epi_week_start")
            .alias("latest_week")
        )
        .collect()[0]["latest_week"]
    )

    print("Latest week:", latest)

    # Keep only the columns actually required.
    select_columns = [
        "district_name",
        "disease",
        "epi_week_start",
        "epi_week",
        "case_count",
        "population_total",
    ]

    for c in available_model_features:
        if c not in select_columns:
            select_columns.append(c)

    df = (
        df.select(*select_columns)
        .withColumn(
            "case_count",
            F.col("case_count").cast("double")
        )
    )

    return df, available_model_features


# ============================================================
# FEATURE IMPUTATION
# ============================================================

def add_safe_features(df, feature_columns):

    """
    Replace missing values with zero.

    Most missing values in this project occur in early lag weeks.
    Zero is a conservative deterministic treatment for the
    backtest and avoids expensive Imputer fitting at every origin.
    """

    for c in feature_columns:
        df = df.withColumn(
            c,
            F.coalesce(
                F.col(c).cast("double"),
                F.lit(0.0)
            )
        )

    return df


# ============================================================
# TRAIN MODEL
# ============================================================

def train_model(training_df, feature_columns):

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="raw_features",
        handleInvalid="keep"
    )

    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False
    )

    regression = LinearRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",

        maxIter=MAX_ITER,

        regParam=REG_PARAM,

        elasticNetParam=ELASTIC_NET_PARAM,

        fitIntercept=True,

        standardization=False
    )

    pipeline = Pipeline(
        stages=[
            assembler,
            scaler,
            regression
        ]
    )

    model = pipeline.fit(
        training_df
    )

    return model


# ============================================================
# BUILD HORIZON DATA
# ============================================================

def create_supervised_data(
    disease_df,
    horizon
):

    """
    Create:

        features at week t
        target at week t+horizon

    The window is partitioned by district because the function
    is already called separately for each disease.
    """

    window = (
        Window
        .partitionBy("district_name")
        .orderBy("epi_week_start")
    )

    target = F.lead(
        "case_count",
        horizon
    ).over(window)

    result = (
        disease_df
        .withColumn(
            "target",
            target
        )
        .withColumn(
            "target_week",
            F.date_add(
                F.col("epi_week_start"),
                horizon * 7
            )
        )
    )

    return result


# ============================================================
# SEASONAL NAIVE
# ============================================================

def build_seasonal_naive(
    disease_df,
    origin,
    horizon
):

    """
    Seasonal-naive:

        forecast(t+h) = observed(t+h-52)

    Since the project has weekly data, a 52-week seasonal lag
    is used.
    """

    seasonal_window = (
        Window
        .partitionBy("district_name")
        .orderBy("epi_week_start")
    )

    naive_df = (
        disease_df
        .withColumn(
            "seasonal_case",
            F.lag(
                "case_count",
                52
            ).over(seasonal_window)
        )
        .filter(
            F.col("epi_week_start") == F.lit(origin)
        )
        .select(
            "district_name",
            F.col("seasonal_case")
            .cast("double")
            .alias("seasonal_prediction")
        )
    )

    return naive_df


# ============================================================
# RESIDUAL INTERVAL
# ============================================================

def calculate_prediction_interval(
    predictions_df
):

    """
    Symmetric 80% interval from absolute residual quantile.

    This is intentionally simple and transparent for the report.
    """

    residual_df = (
        predictions_df
        .withColumn(
            "abs_error",
            F.abs(
                F.col("label")
                - F.col("prediction")
            )
        )
    )

    q80 = collect_quantile(
        residual_df,
        "abs_error",
        0.80
    )

    return q80


# ============================================================
# ONE ORIGIN / ONE HORIZON
# ============================================================

def evaluate_origin_horizon(
    disease_df,
    supervised_df,
    origin,
    horizon,
    feature_columns
):

    # --------------------------------------------------------
    # Training:
    # target must already be known by the origin.
    # --------------------------------------------------------

    training = (
        supervised_df
        .filter(
            F.col("target_week") <= F.lit(origin)
        )
        .filter(
            F.col("target").isNotNull()
        )
        .select(
            "district_name",
            "epi_week_start",
            *feature_columns,
            F.col("target").alias("label")
        )
    )

    training = add_safe_features(
        training,
        feature_columns
    )

    training = (
        training
        .dropna(subset=["label"])
        .repartition(4)
    )

    training_count = training.count()

    if training_count < 500:
        return []

    # --------------------------------------------------------
    # Fit
    # --------------------------------------------------------

    model = train_model(
        training,
        feature_columns
    )

    # --------------------------------------------------------
    # Prediction rows:
    # features observed exactly at origin.
    # --------------------------------------------------------

    prediction_rows = (
        supervised_df
        .filter(
            F.col("epi_week_start") == F.lit(origin)
        )
        .select(
            "district_name",
            F.col("epi_week_start")
            .alias("origin_week"),
            *feature_columns,
            F.col("target")
            .alias("actual")
        )
    )

    prediction_rows = add_safe_features(
        prediction_rows,
        feature_columns
    )

    predictions = (
        model
        .transform(prediction_rows)
        .select(
            "district_name",
            "origin_week",
            F.lit(horizon).alias("horizon"),
            "actual",
            F.col("prediction").cast("double")
        )
    )

    # --------------------------------------------------------
    # Seasonal naive
    # --------------------------------------------------------

    naive = build_seasonal_naive(
        disease_df,
        origin,
        horizon
    )

    predictions = (
        predictions
        .join(
            naive,
            on="district_name",
            how="left"
        )
    )

    # --------------------------------------------------------
    # Collect only small final result.
    # --------------------------------------------------------

    rows = (
        predictions
        .withColumn(
            "prediction",
            F.greatest(
                F.lit(0.0),
                F.col("prediction")
            )
        )
        .collect()
    )

    # --------------------------------------------------------
    # Estimate residual interval from this origin's
    # training predictions.
    #
    # To keep memory bounded, use a small training sample.
    # --------------------------------------------------------

    residual_training = (
        model
        .transform(
            training.limit(8000)
        )
        .select(
            "label",
            "prediction"
        )
        .withColumn(
            "abs_error",
            F.abs(
                F.col("label")
                - F.col("prediction")
            )
        )
    )

    interval_width = collect_quantile(
        residual_training,
        "abs_error",
        0.80
    )

    output = []

    for row in rows:

        actual = safe_float(
            row["actual"]
        )

        prediction = safe_float(
            row["prediction"]
        )

        naive_prediction = safe_float(
            row["seasonal_prediction"]
        )

        if actual is None:
            continue

        if prediction is None:
            prediction = 0.0

        if naive_prediction is None:
            naive_prediction = 0.0

        lower = max(
            0.0,
            prediction - interval_width
        )

        upper = (
            prediction
            + interval_width
        )

        model_error = abs(
            actual - prediction
        )

        naive_error = abs(
            actual - naive_prediction
        )

        covered = (
            lower <= actual <= upper
        )

        output.append(
            (
                row["district_name"],
                str(row["origin_week"]),
                int(horizon),
                actual,
                prediction,
                lower,
                upper,
                bool(covered),
                naive_prediction,
                model_error,
                naive_error,
                int(training_count),
                float(interval_width)
            )
        )

    # Explicitly release Spark model.
    del model
    del training
    del prediction_rows
    del predictions
    del residual_training

    gc.collect()

    return output


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("EpiScope AN-5 Rolling-Origin Backtesting")
    print("=" * 70)

    ensure_output_dirs()

    spark = create_spark()

    try:

        # ----------------------------------------------------
        # Load
        # ----------------------------------------------------

        feature_df, feature_columns = (
            load_feature_mart(spark)
        )

        # ----------------------------------------------------
        # Determine last 52 weekly origins.
        # ----------------------------------------------------

        origins = (
            feature_df
            .select("epi_week_start")
            .distinct()
            .orderBy(
                F.col("epi_week_start").desc()
            )
            .limit(
                BACKTEST_ORIGINS
            )
            .orderBy(
                F.col("epi_week_start").asc()
            )
            .collect()
        )

        origins = [
            row["epi_week_start"]
            for row in origins
        ]

        print()
        print(
            "Backtest origins:",
            len(origins)
        )

        if origins:
            print(
                "First origin:",
                origins[0]
            )

            print(
                "Last origin:",
                origins[-1]
            )

        # ----------------------------------------------------
        # Diseases
        # ----------------------------------------------------

        diseases = [
            row["disease"]
            for row in (
                feature_df
                .select("disease")
                .distinct()
                .orderBy("disease")
                .collect()
            )
        ]

        all_results = []

        # ----------------------------------------------------
        # Disease loop
        # ----------------------------------------------------

        for disease in diseases:

            print()
            print("=" * 70)
            print(
                "BACKTEST DISEASE:",
                disease
            )
            print("=" * 70)

            disease_df = (
                feature_df
                .filter(
                    F.col("disease")
                    == F.lit(disease)
                )
                .select(
                    "district_name",
                    "disease",
                    "epi_week_start",
                    "epi_week",
                    "case_count",
                    *feature_columns
                )
                .repartition(
                    4,
                    "district_name"
                )
                .cache()
            )

            disease_df.count()

            # ------------------------------------------------
            # Build supervised data once per horizon.
            # ------------------------------------------------

            supervised_by_horizon = {}

            for horizon in HORIZONS:

                supervised_by_horizon[horizon] = (
                    create_supervised_data(
                        disease_df,
                        horizon
                    )
                    .cache()
                )

                supervised_by_horizon[
                    horizon
                ].count()

            # ------------------------------------------------
            # Rolling origins
            # ------------------------------------------------

            for index, origin in enumerate(
                origins,
                start=1
            ):

                print()
                print(
                    f"[{index:02d}/{len(origins):02d}] "
                    f"Origin={origin}"
                )

                origin_results = []

                for horizon in HORIZONS:

                    results = evaluate_origin_horizon(
                        disease_df=disease_df,
                        supervised_df=(
                            supervised_by_horizon[
                                horizon
                            ]
                        ),
                        origin=origin,
                        horizon=horizon,
                        feature_columns=feature_columns
                    )

                    for result in results:

                        origin_results.append(
                            (
                                disease,
                                *result
                            )
                        )

                all_results.extend(
                    origin_results
                )

                print(
                    "  Completed 4 horizons"
                )

                # Force Python GC after every origin.
                gc.collect()

            # ------------------------------------------------
            # Release disease data.
            # ------------------------------------------------

            for horizon in HORIZONS:

                supervised_by_horizon[
                    horizon
                ].unpersist()

            disease_df.unpersist()

            gc.collect()

        # ====================================================
        # Convert small final result to Spark DataFrame
        # ====================================================

        result_schema = [
            "disease",
            "district_name",
            "origin_week",
            "horizon",
            "actual",
            "prediction",
            "prediction_lower",
            "prediction_upper",
            "pi_80_covered",
            "seasonal_naive_prediction",
            "model_absolute_error",
            "seasonal_naive_absolute_error",
            "training_rows",
            "interval_width"
        ]

        result_df = spark.createDataFrame(
            all_results,
            result_schema
        )

        # ====================================================
        # Save detailed results
        # ====================================================

        (
            result_df
            .write
            .mode("overwrite")
            .partitionBy(
                "disease",
                "horizon"
            )
            .parquet(
                RESULT_PATH
            )
        )

        # ====================================================
        # Metrics
        # ====================================================

        metric_df = (
            result_df
            .groupBy(
                "disease",
                "horizon"
            )
            .agg(
                F.count("*")
                .alias("evaluation_rows"),

                F.avg(
                    "model_absolute_error"
                )
                .alias("model_mae"),

                F.avg(
                    "seasonal_naive_absolute_error"
                )
                .alias("seasonal_naive_mae"),

                F.avg(
                    F.col("pi_80_covered")
                    .cast("double")
                )
                .alias("pi_80_coverage"),

                F.avg(
                    "interval_width"
                )
                .alias("mean_interval_width"),

                F.avg(
                    "training_rows"
                )
                .alias("mean_training_rows")
            )
            .withColumn(
                "model_better_than_seasonal_naive",
                F.col("model_mae")
                < F.col("seasonal_naive_mae")
            )
            .orderBy(
                "disease",
                "horizon"
            )
        )

        metric_df.show(
            100,
            truncate=False
        )

        (
            metric_df
            .write
            .mode("overwrite")
            .parquet(
                METRIC_PATH
            )
        )

        # ====================================================
        # Overall metrics
        # ====================================================

        overall = (
            result_df
            .agg(
                F.count("*")
                .alias("rows"),

                F.avg(
                    "model_absolute_error"
                )
                .alias("model_mae"),

                F.avg(
                    "seasonal_naive_absolute_error"
                )
                .alias("seasonal_naive_mae"),

                F.avg(
                    F.col("pi_80_covered")
                    .cast("double")
                )
                .alias("pi_80_coverage")
            )
            .collect()[0]
        )

        summary = {
            "project": "EpiScope",
            "analysis": "AN-5 Rolling-Origin Forecast Backtesting",

            "generated_at": datetime.utcnow()
            .isoformat(),

            "feature_mart": FEATURE_PATH,

            "diseases": diseases,

            "origins": len(origins),

            "first_origin": (
                str(origins[0])
                if origins
                else None
            ),

            "last_origin": (
                str(origins[-1])
                if origins
                else None
            ),

            "horizons": HORIZONS,

            "prediction_interval": "80%",

            "model": {
                "algorithm":
                    "Spark ML Elastic-Net Linear Regression",

                "max_iter":
                    MAX_ITER,

                "reg_param":
                    REG_PARAM,

                "elastic_net_param":
                    ELASTIC_NET_PARAM,

                "feature_count":
                    len(feature_columns),

                "features":
                    feature_columns
            },

            "overall": {
                "evaluation_rows":
                    int(overall["rows"]),

                "model_mae":
                    safe_float(
                        overall["model_mae"]
                    ),

                "seasonal_naive_mae":
                    safe_float(
                        overall[
                            "seasonal_naive_mae"
                        ]
                    ),

                "pi_80_coverage":
                    safe_float(
                        overall[
                            "pi_80_coverage"
                        ]
                    )
            },

            "outputs": {
                "detailed_results":
                    RESULT_PATH,

                "metrics":
                    METRIC_PATH,

                "summary":
                    SUMMARY_PATH
            },

            "notes": [
                "Spark is used for feature preparation and model fitting.",
                "AN-5 uses a compact Elastic-Net linear model to keep the 52-origin backtest runnable on a 16 GB laptop.",
                "AN-4 may use a more complex parallel Spark ML model.",
                "Seasonal-naive uses a 52-week seasonal lag.",
                "Prediction intervals are based on the 80th percentile of absolute training residuals.",
                "Ground-truth outbreak data is not used in forecasting."
            ]
        }

        with open(
            SUMMARY_PATH,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                summary,
                f,
                indent=2,
                ensure_ascii=False,
                default=str
            )

        print()
        print("=" * 70)
        print("AN-5 BACKTEST COMPLETE")
        print("=" * 70)

        print(
            "Evaluation rows      :",
            overall["rows"]
        )

        print(
            "Model MAE            :",
            safe_float(
                overall["model_mae"]
            )
        )

        print(
            "Seasonal-naive MAE   :",
            safe_float(
                overall[
                    "seasonal_naive_mae"
                ]
            )
        )

        print(
            "80% PI coverage      :",
            safe_float(
                overall[
                    "pi_80_coverage"
                ]
            )
        )

        print()
        print(
            "Detailed results     :",
            RESULT_PATH
        )

        print(
            "Metrics              :",
            METRIC_PATH
        )

        print(
            "Summary              :",
            SUMMARY_PATH
        )

    finally:

        spark.stop()

        gc.collect()

        print()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()