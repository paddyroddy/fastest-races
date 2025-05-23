import argparse
import io
import logging
import math
import os
import pathlib
import webbrowser

import bs4
import pandas as pd
import urllib3

_logger = logging.getLogger(__name__)

_ERROR_CODES = 400
_SECONDS_HOUR = 3_600
_SECONDS_MINUTE = 60


# --- Helper Functions ---


def _format_seconds_to_display(total_seconds: int) -> str:
    """
    Format total seconds.

    Format total seconds into MM:SS if less than an hour, or H:MM:SS if an hour
    or more. This is used for the 'Fastest' column.
    """
    if total_seconds < _SECONDS_HOUR:
        minutes = int(total_seconds // _SECONDS_MINUTE)
        seconds = int(total_seconds % _SECONDS_MINUTE)
        return f"{minutes:02d}:{seconds:02d}"
    # 1 hour or more
    hours = int(total_seconds // _SECONDS_HOUR)
    minutes_after_hours = int((total_seconds % _SECONDS_HOUR) // _SECONDS_MINUTE)
    seconds = int(total_seconds % _SECONDS_MINUTE)
    # Using 'd' for hours allows single digit for 1-9 hours (e.g., 1:05:00)
    # Change to '{hours:02d}' if you always want two digits (e.g., 01:05:00)
    return f"{hours:d}:{minutes_after_hours:02d}:{seconds:02d}"


def _format_threshold_minutes_to_display(threshold_min: int) -> str:
    """
    Format a minute threshold.

    Format a minute threshold into a readable string (e.g., '< 30', '< 1:00').
    Displays as <H:MM if threshold is 60 minutes or more, else <M. This is used
    for the column titles.
    """
    if (
        threshold_min >= _SECONDS_MINUTE
    ):  # If the threshold itself is 60 minutes or more
        threshold_hours = threshold_min // _SECONDS_MINUTE
        threshold_remainder_minutes = threshold_min % _SECONDS_MINUTE
        return f"< {threshold_hours:d}:{threshold_remainder_minutes:02d}"
    # For thresholds less than 60 minutes (e.g., < 30, < 45)
    return f"< {threshold_min:d}"


# --- Main Data Processing Functions ---


def get_ranking_data(gender: str, year: str, distance: str) -> pd.DataFrame:
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
    url = f"https://www.thepowerof10.info/rankings/rankinglist.aspx?event={distance}&agegroup=ALL&sex={gender}&year={year}"

    http = urllib3.PoolManager()
    try:
        response = http.request("GET", url)

        if response.status >= _ERROR_CODES:
            msg = f"HTTP Error {response.status}: Failed to fetch data from {url}"
            raise ConnectionError(msg)

    except urllib3.exceptions.MaxRetryError as e:
        msg = f"Could not connect to {url}: {e}"
        raise ConnectionError(msg) from e
    except Exception as e:
        msg = f"An unexpected error occurred during data fetch: {e}"
        raise ConnectionError(msg) from e

    html_content = response.data.decode("utf-8")
    soup = bs4.BeautifulSoup(html_content, "html.parser")

    table_span = soup.find("span", {"id": "cphBody_lblCachedRankingList"})
    if not table_span:
        msg = (
            f"Could not find the ranking list table container (span with ID "
            f"'cphBody_lblCachedRankingList') on {url}. The page structure "
            "might have changed or no data for this query."
        )
        raise ValueError(msg)

    table = table_span.find("table")
    if not table:
        msg = (
            f"Could not find a table within the ranking list span on {url}. "
            "This might mean no data is available for your query or the page "
            "structure has changed."
        )
        raise ValueError(msg)

    try:
        df = pd.read_html(io.StringIO(str(table)), header=1)[0]
    except ValueError as e:
        msg = (
            "Failed to parse table from HTML content. This might happen if "
            f"the table is empty or malformed. Original error: {e}"
        )
        raise ValueError(msg) from e

    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    df = df.loc[df["Perf"].str.match(r"^\d+:\d{2}(:\d{2})?$")].copy()

    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("Perf").reset_index(drop=True)

    columns_to_drop_final = ["Gun", "PB", "Name", "Coach", "Club", "Rank"]
    df = df.drop(columns=columns_to_drop_final, errors="ignore")

    split_venue_country = df["Venue"].str.split(",", n=1, expand=True)
    df["Venue"] = split_venue_country[0]
    df["Country"] = split_venue_country.get(1, pd.Series(dtype=str))
    df["Country"] = df["Country"].fillna("UK").str.strip()

    df["Perf_seconds"] = df["Perf"].apply(
        lambda x: sum(
            int(part) * (_SECONDS_MINUTE**i)
            for i, part in enumerate(reversed(x.split(":")))
        )
    )

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%d %b %y")
    else:
        msg = (
            "The 'Date' column was not found after data processing. Cannot "
            "group results."
        )
        raise ValueError(msg)

    return df


def calculate_performance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate performance metrics based on the provided DataFrame.

    Calculate dynamic minute thresholds and aggregates performance counts
    based on those thresholds. Also flattens the multi-index for a single-row header
    and reorders columns to place 'Fastest' after 'Country'.

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

    start_minute_threshold = math.floor(global_min_seconds / _SECONDS_MINUTE) + 1
    end_minute_threshold = math.ceil(global_max_seconds / _SECONDS_MINUTE)

    end_minute_threshold = max(start_minute_threshold, end_minute_threshold)

    dynamic_threshold_minutes = list(
        range(start_minute_threshold, end_minute_threshold + 1)
    ) or [math.ceil(global_min_seconds / _SECONDS_MINUTE)]

    def _aggregate_dynamic_thresholds(group: pd.DataFrame) -> pd.Series:
        results = {}
        for threshold_min in dynamic_threshold_minutes:
            # Call _format_threshold_minutes_to_display WITHOUT distance parameter
            col_name = _format_threshold_minutes_to_display(threshold_min)
            threshold_seconds_val = threshold_min * _SECONDS_MINUTE
            results[col_name] = (group["Perf_seconds"] < threshold_seconds_val).sum()

        min_perf_seconds = group["Perf_seconds"].min()
        # Call _format_seconds_to_display WITHOUT distance parameter
        results["Fastest"] = _format_seconds_to_display(min_perf_seconds)

        return pd.Series(results)

    grouped = df.groupby(["Date", "Venue", "Country"], dropna=False)
    output = grouped.apply(_aggregate_dynamic_thresholds, include_groups=False)

    # Use the general formatting for sort_columns_for_display
    sort_columns_for_display = [
        _format_threshold_minutes_to_display(m) for m in dynamic_threshold_minutes
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
        _format_threshold_minutes_to_display(m) for m in dynamic_threshold_minutes
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


def generate_and_open_html_report(
    output_df: pd.DataFrame,
    css_file: str = "simple_table.css",
    html_file: str = "performance_analysis.html",
) -> None:
    """
    Generate an HTML report from the DataFrame and opens it in the default browser.

    Parameters
    ----------
    output_df
        The final DataFrame to be displayed.
    css_file
        The name of the CSS stylesheet file.
    html_file
        The name of the HTML file to be generated.

    """
    html_table = output_df.to_html(index=False, classes="styled-table")

    min_date_str = "N/A"
    max_date_str = "N/A"
    if not output_df.empty and "Date" in output_df.columns:
        min_date_str = output_df["Date"].min()
        max_date_str = output_df["Date"].max()

    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Performance Analysis</title>
    <link rel="stylesheet" type="text/css" href="{css_file}">
</head>
<body>
    <h1>Performance Analysis Results</h1>
    <p>Data from {min_date_str} to {max_date_str}</p>
    {html_table}
</body>
</html>
"""

    with pathlib.Path.open(html_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    msg = f"Opening '{html_file}' in your default browser..."
    _logger.info(msg)
    msg = f"file://{os.path.realpath(html_file)}"
    webbrowser.open(msg)
    msg = (
        f"Make sure you have a file named '{css_file}' in the same directory "
        "as your Python script with the CSS content provided previously."
    )
    _logger.info(msg)


# --- Main Execution Logic ---


def main() -> None:
    """Parse arguments, fetch, process, and display athlete performance data."""
    parser = argparse.ArgumentParser(
        description="Fetch and analyze athlete performance data from The Power of 10."
    )
    parser.add_argument(
        "-g",
        "--gender",
        type=str,
        choices=["M", "F"],
        required=True,
        help="Gender for the rankings (M for male, F for female).",
    )
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        required=True,
        help="Year for the rankings (e.g., 2024).",
    )
    parser.add_argument(
        "-d",
        "--distance",
        type=str,
        choices=["10K", "HM", "Mar", "5K"],
        required=True,
        help="Distance of the event (10K, Half Marathon, Marathon, 5K).",
    )

    args = parser.parse_args()

    year_str = str(args.year)

    msg = f"Fetching data for {args.gender} {args.distance} in {year_str}..."
    _logger.info(msg)
    try:
        df = get_ranking_data(args.gender, year_str, args.distance)

        if df.empty:
            msg = (
                f"No valid performance data found for {args.gender} "
                f"{args.distance} in {year_str}. This might be due to a "
                "mismatch in `Perf` format, no data available, or an issue "
                "with the website's structure for this query."
            )
            _logger.error(msg)
            return

        output_df = calculate_performance_metrics(df)

        if output_df.empty:
            msg = (
                "No aggregated performance metrics could be calculated for "
                f"{args.gender} {args.distance} in {year_str}. Output "
                "DataFrame is empty."
            )
            _logger.error(msg)
            return

        generate_and_open_html_report(output_df)

    except (ConnectionError, ValueError) as e:
        msg = f"Error: {e}"
        _logger.exception(msg)
    except Exception as e:
        msg = f"An unexpected error occurred: {e}"
        _logger.exception(msg)


if __name__ == "__main__":
    main()
