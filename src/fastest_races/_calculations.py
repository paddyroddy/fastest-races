import io
import math

import pandas as pd

import fastest_races._scraping
import fastest_races._times
from fastest_races._vars import MINUTE_SECONDS


def get_ranking_data(gender: str, year: int, distance: str) -> pd.DataFrame:
    """
    Fetch ranking data from The Power of 10 website and performs initial cleaning.

    Parameters
    ----------
    gender
        The gender to filter by ("M" or "F").
    year
        The year for the rankings (e.g., "2024").
    distance
        The distance of the event ("10K", "Half Marathon", "Marathon", "5K").

    Returns
    -------
        A DataFrame containing the cleaned performance data.

    Raises
    ------
    ValueError
        If the table data cannot be found or parsed from the URL.
    ConnectionError
        If there's an issue fetching data from the URL (e.g., network error, bad
        HTTP status).

    """
    try:
        rankings = pd.read_html(
            io.StringIO(
                str(fastest_races._scraping.get_html_table(gender, year, distance))
            ),
            header=1,
        )[0]
    except ValueError as e:
        msg = (
            "Failed to parse table from HTML content. This might happen if "
            f"the table is empty or malformed. Original error: {e}"
        )
        raise ValueError(msg) from e

    named_cols_only = rankings.loc[:, ~rankings.columns.str.startswith("Unnamed")]

    timed_rows_only = named_cols_only.loc[
        named_cols_only["Perf"].str.match(r"^\d+:\d{2}(:\d{2})?$")
    ].copy()

    if timed_rows_only.empty:
        return pd.DataFrame()

    index_reset = timed_rows_only.sort_values("Perf").reset_index(drop=True)

    columns_to_drop_final = ["Gun", "PB", "Name", "Coach", "Club", "Rank"]
    wanted_cols_only = index_reset.drop(columns=columns_to_drop_final, errors="ignore")

    split_venue_country = wanted_cols_only["Venue"].str.split(",", n=1, expand=True)
    wanted_cols_only["Venue"] = split_venue_country[0]
    wanted_cols_only["Country"] = split_venue_country.get(1, pd.Series(dtype=str))
    wanted_cols_only["Country"] = wanted_cols_only["Country"].fillna("UK").str.strip()

    wanted_cols_only["Perf_seconds"] = wanted_cols_only["Perf"].apply(
        lambda x: sum(
            int(part) * (MINUTE_SECONDS**i)
            for i, part in enumerate(reversed(x.split(":")))
        )
    )

    if "Date" in wanted_cols_only.columns:
        wanted_cols_only["Date"] = pd.to_datetime(
            wanted_cols_only["Date"], format="%d %b %y"
        )
    else:
        msg = (
            "The 'Date' column was not found after data processing. Cannot "
            "group results."
        )
        raise ValueError(msg)

    return wanted_cols_only


def calculate_performance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate performance metrics based on the provided DataFrame.

    Calculate dynamic minute thresholds and aggregates performance counts
    based on those thresholds. Also flattens the multi-index for a single-row
    header and reorders columns to place 'Fastest' after 'Country'.

    Parameters
    ----------
    df
        The preprocessed DataFrame containing 'Perf_seconds' and grouping columns.
    distance
        The event distance. Although no longer used for formatting in helpers,
        it's kept here as a parameter for consistency in the calling signature
        or if future distance-specific logic is added (e.g. for scraping).

    Returns
    -------
        An aggregated DataFrame with counts for each dynamic threshold,
        and a flattened single-row header with 'Fastest' in the desired position.

    """
    if df.empty:
        return pd.DataFrame(columns=["Date", "Venue", "Country", "Fastest"])

    global_min_seconds = df["Perf_seconds"].min()
    global_max_seconds = df["Perf_seconds"].max()

    start_minute_threshold = math.floor(global_min_seconds / MINUTE_SECONDS) + 1
    end_minute_threshold = math.ceil(global_max_seconds / MINUTE_SECONDS)

    end_minute_threshold = max(start_minute_threshold, end_minute_threshold)

    dynamic_threshold_minutes = list(
        range(start_minute_threshold, end_minute_threshold + 1)
    ) or [math.ceil(global_min_seconds / MINUTE_SECONDS)]

    def _aggregate_dynamic_thresholds(group: pd.DataFrame) -> pd.Series:
        results = {}
        for threshold_min in dynamic_threshold_minutes:
            # Call _format_threshold_minutes_to_display WITHOUT distance parameter
            col_name = fastest_races._times.format_threshold_minutes_to_display(
                threshold_min
            )
            threshold_seconds_val = threshold_min * MINUTE_SECONDS
            results[col_name] = (group["Perf_seconds"] < threshold_seconds_val).sum()

        min_perf_seconds = group["Perf_seconds"].min()
        # Call _format_seconds_to_display WITHOUT distance parameter
        results["Fastest"] = fastest_races._times.format_seconds_to_display(
            min_perf_seconds
        )

        return pd.Series(results)

    grouped = df.groupby(["Date", "Venue", "Country"], dropna=False)
    output = grouped.apply(_aggregate_dynamic_thresholds, include_groups=False)

    # Use the general formatting for sort_columns_for_display
    sort_columns_for_display = [
        fastest_races._times.format_threshold_minutes_to_display(m)
        for m in dynamic_threshold_minutes
    ]
    sort_columns_for_display.append("Fastest")

    ascending_order = [False] * len(dynamic_threshold_minutes) + [True]

    existing_sort_columns = [
        col for col in sort_columns_for_display if col in output.columns
    ]
    if existing_sort_columns:
        output = output.sort_values(
            by=existing_sort_columns,
            ascending=ascending_order[: len(existing_sort_columns)],
        )

    output = output.reset_index()

    output["Date"] = output["Date"].dt.strftime("%d %b %Y")

    current_columns = output.columns.tolist()

    desired_order_start = ["Date", "Venue", "Country"]
    desired_order_middle = ["Fastest"]

    # Use the general formatting for dynamic_threshold_cols_for_order
    dynamic_threshold_cols_for_order = [
        fastest_races._times.format_threshold_minutes_to_display(m)
        for m in dynamic_threshold_minutes
    ]

    final_column_order = (
        desired_order_start + desired_order_middle + dynamic_threshold_cols_for_order
    )

    final_column_order = [col for col in final_column_order if col in current_columns]

    remaining_columns = [
        col for col in current_columns if col not in final_column_order
    ]
    final_column_order.extend(remaining_columns)

    return output[final_column_order]
