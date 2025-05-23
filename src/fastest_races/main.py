import io
import math
import os
import webbrowser
from datetime import datetime

import bs4
import pandas as pd
import urllib3


def get_ranking_data(gender: str, year: str, distance: str) -> pd.DataFrame:
    """
    Fetches ranking data from The Power of 10 website and performs initial cleaning.

    Parameters
    ----------
    gender : str
        The gender to filter by (e.g., "M" for male, "F" for female).
    year : str
        The year for the rankings (e.g., "2024").
    distance : str
        The distance of the event (e.g., "Marathon", "10K").

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
    # Adjust URL for "Marathon" to "Mar" as per Power of 10 convention if not already done
    # The `distance` parameter handles this, but worth noting if debugging manually.
    # The error message `Workspaceing data for M Mar in 2024...` implies distance was already "Mar"
    
    url = f"https://www.thepowerof10.info/rankings/rankinglist.aspx?event={distance}&agegroup=ALL&sex={gender}&year={year}"
    
    http = urllib3.PoolManager()
    try:
        response = http.request("GET", url)
        
        if response.status >= 400:
            raise ConnectionError(f"HTTP Error {response.status}: Failed to fetch data from {url}")

    except urllib3.exceptions.MaxRetryError as e:
        raise ConnectionError(f"Could not connect to {url}: {e}") from e
    except Exception as e:
        raise ConnectionError(f"An unexpected error occurred during data fetch: {e}") from e

    html_content = response.data.decode("utf-8")
    soup = bs4.BeautifulSoup(html_content, "html.parser")

    table_span = soup.find("span", {"id": "cphBody_lblCachedRankingList"})
    if not table_span:
        raise ValueError(f"Could not find the ranking list table on {url}. "
                         "The page structure might have changed or no data for this query.")
    
    table = table_span.find("table")
    if not table:
        raise ValueError(f"Could not find a table within the ranking list span on {url}.")

    df = pd.read_html(io.StringIO(str(table)), header=1)[0]

    if 'Unnamed: 0' in df.columns and (df['Unnamed: 0'].dtype == 'int64' or pd.api.types.is_numeric_dtype(df['Unnamed: 0'])):
        df = df.rename(columns={'Unnamed: 0': 'Rank'})
    
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    df = df.loc[df["Perf"].str.match(r"^\d+:\d{2}$|^\d+:\d{2}:\d{2}$")].copy() # Adjusted regex for hours:minutes:seconds

    df = df.sort_values("Perf").reset_index(drop=True)

    columns_to_drop_final = ["Gun", "PB", "Name", "Coach", "Club", "Rank"]
    df = df.drop(columns=columns_to_drop_final, errors='ignore')

    # --- FIX for Venue/Country split: Apply the split, then handle separately ---
    # Perform the split and assign to temporary columns or directly
    split_venue_country = df["Venue"].str.split(",", n=1, expand=True)
    
    # Assign the first part (Venue)
    df["Venue"] = split_venue_country[0]
    
    # Assign the second part (Country), defaulting to 'UK' if it doesn't exist (i.e., only one part after split)
    # Use .get(1) to safely access the second column (index 1) which might not exist
    df["Country"] = split_venue_country.get(1, pd.Series(dtype=str)) # Default to empty Series if column 1 doesn't exist
    df["Country"] = df["Country"].fillna("UK").str.strip() # Fill NA from original split or from .get()
    # --- End of FIX ---

    # Also, Marathons can have times like HH:MM:SS, so the Perf_seconds conversion
    # and the Perf regex need to be more robust.
    df["Perf_seconds"] = df["Perf"].apply(
        lambda x: sum(int(part) * (60 ** i) for i, part in enumerate(reversed(x.split(":"))))
    )
    # The previous lambda was: lambda x: int(x.split(":")[0]) * 60 + int(x.split(":")[1])
    # This updated lambda handles MM:SS and HH:MM:SS correctly by parsing from right to left.

    
    if 'Date' in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%d %b %y")
    else:
        raise ValueError("The 'Date' column was not found after data processing. Cannot group results.")
    
    return df


def calculate_performance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates dynamic minute thresholds and aggregates performance counts
    based on those thresholds. Also flattens the multi-index for a single-row header
    and reorders columns to place 'Fastest' after 'Country'.

    Parameters
    ----------
    df : pd.DataFrame
        The preprocessed DataFrame containing 'Perf_seconds' and grouping columns.

    Returns
    -------
    pd.DataFrame
        An aggregated DataFrame with counts for each dynamic threshold,
        and a flattened single-row header with 'Fastest' in the desired position.
    """
    # Ensure there's data before calculating min/max, otherwise it can raise errors
    if df.empty:
        return pd.DataFrame(columns=['Date', 'Venue', 'Country', 'Fastest']) # Return an empty df with expected columns

    global_min_seconds = df["Perf_seconds"].min()
    global_max_seconds = df["Perf_seconds"].max()

    start_minute_threshold = math.floor(global_min_seconds / 60) + 1
    end_minute_threshold = math.ceil(global_max_seconds / 60)

    # For Marathons, thresholds will be in hours. We need to be careful with '< XX' format.
    # It might be better to have '< 3:00', '< 3:05' etc. rather than just '< 180'.
    # If the user only wants minute granularity, this is fine, but it will be a high number.
    # For now, keeping minute thresholds but be aware of how they're displayed.
    dynamic_threshold_minutes = list(
        range(start_minute_threshold, end_minute_threshold + 1)
    )

    def _aggregate_dynamic_thresholds(group):
        results = {}
        for threshold_min in dynamic_threshold_minutes:
            # For Marathon, `threshold_min` could be > 60. E.g., < 180 (for 3 hours)
            # You might want to display this as "< 3:00" for clarity in the HTML.
            threshold_seconds_val = threshold_min * 60
            
            # --- Optional: Format threshold column names for readability in hours:minutes ---
            threshold_hours = threshold_min // 60
            threshold_remainder_minutes = threshold_min % 60
            if threshold_hours > 0:
                col_name = f"< {threshold_hours:d}:{threshold_remainder_minutes:02d}"
            else:
                col_name = f"< {threshold_min:d}" # For times under an hour
            # --- End Optional Formatting ---

            results[col_name] = (group["Perf_seconds"] < threshold_seconds_val).sum()

        min_perf_seconds = group["Perf_seconds"].min()
        total_hours = int(min_perf_seconds // 3600) # Calculate total hours
        remaining_minutes = int((min_perf_seconds % 3600) // 60) # Calculate remaining minutes
        remaining_seconds = int(min_perf_seconds % 60) # Calculate remaining seconds

        if total_hours > 0:
            results["Fastest"] = f"{total_hours:02d}:{remaining_minutes:02d}:{remaining_seconds:02d}"
        else:
            results["Fastest"] = f"{remaining_minutes:02d}:{remaining_seconds:02d}"
        
        return pd.Series(results)

    grouped = df.groupby(["Date", "Venue", "Country"], dropna=False)
    output = grouped.apply(_aggregate_dynamic_thresholds, include_groups=False)

    # Adjust sort_columns based on the new threshold name format
    # This needs to come AFTER output is created, so `output.columns` can be inspected.
    # Or, regenerate based on `dynamic_threshold_minutes` and the formatting logic.
    sort_columns_for_display = []
    for threshold_min in dynamic_threshold_minutes:
        threshold_hours = threshold_min // 60
        threshold_remainder_minutes = threshold_min % 60
        if threshold_hours > 0:
            sort_columns_for_display.append(f"< {threshold_hours:d}:{threshold_remainder_minutes:02d}")
        else:
            sort_columns_for_display.append(f"< {threshold_min:d}")
    
    sort_columns_for_display.append("Fastest")

    # The sorting for 'Fastest' needs to be handled carefully.
    # Sorting by `Fastest` string ("02:59:00") will not work correctly if "1:59:00" vs "2:00:00" exists.
    # It's better to sort by the underlying `Perf_seconds` if we had it.
    # Since we can't easily retrieve `Perf_seconds` from `output` directly,
    # we'll sort primarily by count (descending), then by Fastest (ascending).
    # If a deeper numerical sort is needed, we'd need to re-introduce the `Perf_seconds`
    # or a similar numeric representation into `output`.

    # For now, let's revert to a slightly simpler sorting for 'Fastest'
    # if it's treated as string, or ensure `Fastest` is always H:M:S for consistent string sort.
    # The previous `ascending_order = [False] * len(dynamic_threshold_minutes) + [True]` is generally fine
    # if all times are formatted consistently (e.g., HH:MM:SS).
    
    # If the fastest column now contains hours, we need to adjust the sorting for it.
    # The string representation "02:59:00" will sort correctly compared to "03:00:00".
    # So `ascending=True` for 'Fastest' is fine.
    
    ascending_order = [False] * len(dynamic_threshold_minutes) + [True]
    output = output.sort_values(by=sort_columns_for_display, ascending=ascending_order)
    
    output = output.reset_index()

    output['Date'] = output['Date'].dt.strftime('%d %b %Y')

    current_columns = output.columns.tolist()
    
    desired_order_start = ['Date', 'Venue', 'Country']
    desired_order_middle = ['Fastest']
    
    # Use the formatted dynamic threshold column names for ordering
    dynamic_threshold_cols_for_order = [col for col in sort_columns_for_display if col.startswith('< ')]

    final_column_order = desired_order_start + desired_order_middle + dynamic_threshold_cols_for_order

    final_column_order = [col for col in final_column_order if col in current_columns]
    
    remaining_columns = [col for col in current_columns if col not in final_column_order]
    final_column_order.extend(remaining_columns)

    output = output[final_column_order]

    return output

def generate_and_open_html_report(output_df: pd.DataFrame, css_file: str = "simple_table.css", html_file: str = "performance_analysis.html"):
    # ... (this function remains unchanged) ...
    html_table = output_df.to_html(index=False, classes="styled-table") 
    
    min_date_str = "N/A"
    max_date_str = "N/A"
    if not output_df.empty and 'Date' in output_df.columns:
        min_date_str = output_df['Date'].min() 
        max_date_str = output_df['Date'].max()


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
    print(f"\nMake sure you have a file named '{css_file}' in the same directory "
          "as your Python script with the CSS content provided previously.")


def main(gender: str, year: str, distance: str) -> None:
    # ... (this function remains unchanged) ...
    print(f"Fetching data for {gender} {distance} in {year}...")
    try:
        df = get_ranking_data(gender, year, distance)
        
        if df.empty:
            print(f"No valid performance data found for {gender} {distance} in {year}. This might be due to a mismatch in `Perf` format or no data available.")
            return

        output_df = calculate_performance_metrics(df)
        generate_and_open_html_report(output_df)
        
    except (ConnectionError, ValueError) as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Test with Half Marathon (to confirm previous fix)
    # main("M", "2024", "Half Marathon")

    # Test with Marathon
    main("M", "2024", "Mar")

    # Test with 10K
    # main("M", "2024", "10K")