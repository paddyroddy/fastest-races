import argparse
import io
import math
import os
import webbrowser

import bs4
import pandas as pd
import urllib3

HOUR = 3_600
MINUTE = 60

# --- Helper Functions ---


def _format_seconds_to_display(total_seconds: int) -> str:
    """
    Format total seconds into MM:SS if less than an hour, or H:MM:SS if an hour or more.
    This is used for the 'Fastest' column.
    """
    if total_seconds < HOUR:
        minutes = int(total_seconds // MINUTE)
        seconds = int(total_seconds % MINUTE)
        return f"{minutes:02d}:{seconds:02d}"
    # 1 hour or more
    hours = int(total_seconds // HOUR)
    minutes_after_hours = int((total_seconds % HOUR) // MINUTE)
    seconds = int(total_seconds % MINUTE)
    # Using 'd' for hours allows single digit for 1-9 hours (e.g., 1:05:00)
    # Change to '{hours:02d}' if you always want two digits (e.g., 01:05:00)
    return f"{hours:d}:{minutes_after_hours:02d}:{seconds:02d}"


def _format_threshold_minutes_to_display(threshold_min: int) -> str:
    """
    Format a minute threshold into a readable string (e.g., '< 30', '< 1:00').
    Displays as <H:MM if threshold is 60 minutes or more, else <M.
    This is used for the column titles.
    """
    if threshold_min >= MINUTE:  # If the threshold itself is 60 minutes or more
        threshold_hours = threshold_min // MINUTE
        threshold_remainder_minutes = threshold_min % MINUTE
        return f"< {threshold_hours:d}:{threshold_remainder_minutes:02d}"
    # For thresholds less than 60 minutes (e.g., < 30, < 45)
    return f"< {threshold_min:d}"


# --- Main Data Processing Functions ---


def get_ranking_data(gender: str, year: str, distance: str) -> pd.DataFrame:
    """
    Fetch ranking data from The Power of 10 website and performs initial cleaning.

    Parameters
    ----------
    gender : str
        The gender to filter by ("M" or "F").
    year : str
        The year for the rankings (e.g., "2024").
    distance : str
        The distance of the event ("10K", "Half Marathon", "Marathon", "5K").

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the cleaned performance data.

    Raises
    ------
    ValueError
        If the table data cannot be found or parsed from the URL.
    ConnectionError
        If there's an issue fetching data from the URL (e.g., network error, bad HTTP status).

    """
    url = f"https://www.thepowerof10.info/rankings/rankinglist.aspx?event={distance}&agegroup=ALL&sex={gender}&year={year}"

    http = urllib3.PoolManager()
    try:
        response = http.request("GET", url)

        if response.status >= 400:
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
        msg = f"Could not find the ranking list table container (span with ID 'cphBody_lblCachedRankingList') on {url}. The page structure might have changed or no data for this query."
        raise ValueError(msg)

    table = table_span.find("table")
    if not table:
        msg = f"Could not find a table within the ranking list span on {url}. This might mean no data is available for your query or the page structure has changed."  # noqa: E501
        raise ValueError(msg)

    try:
        df = pd.read_html(io.StringIO(str(table)), header=1)[0]
    except ValueError as e:
        msg = f"Failed to parse table from HTML content. This might happen if the table is empty or malformed. Original error: {e}"
        raise ValueError(msg) from e

    if "Unnamed: 0" in df.columns and (
        df["Unnamed: 0"].dtype == "int64"
        or pd.api.types.is_numeric_dtype(df["Unnamed: 0"])
    ):
        df = df.rename(columns={"Unnamed: 0": "Rank"})

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
            int(part) * (MINUTE**i) for i, part in enumerate(reversed(x.split(":")))
        )
    )

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%d %b %y")
    else:
        raise ValueError(
            "The 'Date' column was not found after data processing. Cannot group results."
        )

    return df


def calculate_performance_metrics(df: pd.DataFrame, distance: str) -> pd.DataFrame:
    """
    Calculates dynamic minute thresholds and aggregates performance counts
    based on those thresholds. Also flattens the multi-index for a single-row header
    and reorders columns to place 'Fastest' after 'Country'.

    Parameters
    ----------
    df : pd.DataFrame
        The preprocessed DataFrame containing 'Perf_seconds' and grouping columns.
    distance : str
        The event distance. Although no longer used for formatting in helpers,
        it's kept here as a parameter for consistency in the calling signature
        or if future distance-specific logic is added (e.g. for scraping).

    Returns
    -------
    pd.DataFrame
        An aggregated DataFrame with counts for each dynamic threshold,
        and a flattened single-row header with 'Fastest' in the desired position.

    """
    if df.empty:
        return pd.DataFrame(columns=["Date", "Venue", "Country", "Fastest"])

    global_min_seconds = df["Perf_seconds"].min()
    global_max_seconds = df["Perf_seconds"].max()

    start_minute_threshold = math.floor(global_min_seconds / MINUTE) + 1
    end_minute_threshold = math.ceil(global_max_seconds / MINUTE)

    end_minute_threshold = max(start_minute_threshold, end_minute_threshold)

    dynamic_threshold_minutes = list(
        range(start_minute_threshold, end_minute_threshold + 1)
    ) or [math.ceil(global_min_seconds / MINUTE)]

    def _aggregate_dynamic_thresholds(group):
        results = {}
        for threshold_min in dynamic_threshold_minutes:
            # Call _format_threshold_minutes_to_display WITHOUT distance parameter
            col_name = _format_threshold_minutes_to_display(threshold_min)
            threshold_seconds_val = threshold_min * MINUTE
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
):
    """
    Generate an HTML report from the DataFrame and opens it in the default browser.

    Parameters
    ----------
    output_df : pd.DataFrame
        The final DataFrame to be displayed.
    css_file : str
        The name of the CSS stylesheet file.
    html_file : str
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

    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Opening '{html_file}' in your default browser...")
    webbrowser.open(f"file://{os.path.realpath(html_file)}")
    print(
        f"\nMake sure you have a file named '{css_file}' in the same directory "
        "as your Python script with the CSS content provided previously."
    )


# --- Main Execution Logic ---


def main() -> None:
    """
    Main function to parse arguments, fetch, process, and display athlete performance data.
    """
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

    print(f"Fetching data for {args.gender} {args.distance} in {year_str}...")
    try:
        df = get_ranking_data(args.gender, year_str, args.distance)

        if df.empty:
            print(
                f"No valid performance data found for {args.gender} {args.distance} in {year_str}. "
                "This might be due to a mismatch in `Perf` format, no data available, or an issue with the website's structure for this query."
            )
            return

        # Distance is still passed here for get_ranking_data's URL, but no longer used in formatting.
        output_df = calculate_performance_metrics(df, args.distance)

        if output_df.empty:
            print(
                f"No aggregated performance metrics could be calculated for {args.gender} {args.distance} in {year_str}. Output DataFrame is empty."
            )
            return

        generate_and_open_html_report(output_df)

    except (ConnectionError, ValueError) as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
