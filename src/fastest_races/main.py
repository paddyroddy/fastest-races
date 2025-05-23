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
        The distance of the event (e.g., "10K", "Marathon").

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

    # Use Pandas to read the HTML table into a DataFrame
    df = pd.read_html(io.StringIO(str(table)), header=1)[0]

    # Rename 'Unnamed: 0' to 'Rank' if it exists and is indeed the Rank column
    if 'Unnamed: 0' in df.columns and (df['Unnamed: 0'].dtype == 'int64' or pd.api.types.is_numeric_dtype(df['Unnamed: 0'])):
        df = df.rename(columns={'Unnamed: 0': 'Rank'})
    else:
        # Fallback for pages that might not have an unnamed column 0, or it's not Rank
        pass 

    df = df.loc[:, ~df.columns.str.startswith("Unnamed")] # Re-apply after potential rename


    # Filter for valid time formats - crucial to do before performance calculations
    df = df.loc[df["Perf"].str.match(r"^\d+:\d{2}$")].copy()

    # Sort by performance and reset index
    df = df.sort_values("Perf").reset_index(drop=True)

    # Columns to drop after renaming Unnamed:0 (if it was Rank)
    columns_to_drop_final = ["Gun", "PB", "Name", "Coach", "Club", "Rank"]
    df = df.drop(columns=columns_to_drop_final, errors='ignore')

    df[["Venue", "Country"]] = df["Venue"].str.split(",", n=1, expand=True)
    df["Country"] = df["Country"].fillna("UK").str.strip()

    df["Perf_seconds"] = df["Perf"].apply(
        lambda x: int(x.split(":")[0]) * 60 + int(x.split(":")[1])
    )
    
    if 'Date' in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%d %b %y")
    else:
        raise ValueError("The 'Date' column was not found after data processing. Cannot group results.")
    
    return df

def calculate_performance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates dynamic minute thresholds and aggregates performance counts
    based on those thresholds.

    Parameters
    ----------
    df : pd.DataFrame
        The preprocessed DataFrame containing 'Perf_seconds' and grouping columns.

    Returns
    -------
    pd.DataFrame
        An aggregated DataFrame with counts for each dynamic threshold.
    """
    global_min_seconds = df["Perf_seconds"].min()
    global_max_seconds = df["Perf_seconds"].max()

    start_minute_threshold = math.floor(global_min_seconds / 60) + 1
    end_minute_threshold = math.ceil(global_max_seconds / 60)

    dynamic_threshold_minutes = list(
        range(start_minute_threshold, end_minute_threshold + 1)
    )

    def _aggregate_dynamic_thresholds(group):
        """
        Inner helper function for custom aggregation on each group.
        """
        results = {}
        for threshold_min in dynamic_threshold_minutes:
            col_name = f"< {threshold_min}"
            threshold_seconds = threshold_min * 60
            results[col_name] = (group["Perf_seconds"] < threshold_seconds).sum()

        results["min"] = pd.to_datetime(group["Perf_seconds"].min(), unit="s").strftime("%M:%S")
        return pd.Series(results)

    grouped = df.groupby(["Date", "Venue", "Country"], dropna=False)
    # FIX: Add include_groups=False to silence the DeprecationWarning
    output = grouped.apply(_aggregate_dynamic_thresholds, include_groups=False)

    sort_columns = [f"< {m}" for m in dynamic_threshold_minutes]
    sort_columns.append("min")

    output = output.sort_values(by=sort_columns, ascending=False)
    
    return output

def generate_and_open_html_report(output_df: pd.DataFrame, css_file: str = "simple_table.css", html_file: str = "performance_analysis.html"):
    """
    Generates an HTML report from the DataFrame and opens it in the default browser.

    Parameters
    ----------
    output_df : pd.DataFrame
        The final DataFrame to be displayed.
    css_file : str
        The name of the CSS stylesheet file.
    html_file : str
        The name of the HTML file to be generated.
    """
    html_table = output_df.to_html(index=True, classes="styled-table")

    min_date_str = "N/A"
    max_date_str = "N/A"
    if not output_df.empty and 'Date' in output_df.index.names:
        min_date = output_df.index.get_level_values('Date').min()
        max_date = output_df.index.get_level_values('Date').max()
        if pd.notna(min_date):
            min_date_str = min_date.strftime('%d %b %Y')
        if pd.notna(max_date):
            max_date_str = max_date.strftime('%d %b %Y')

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
    """
    Main function to fetch, process, and display athlete performance data.

    Parameters
    ----------
    gender : str
        The gender to filter by (e.g., "M" for male, "F" for female).
    year : str
        The year for the rankings (e.g., "2024").
    distance : str
        The distance of the event (e.g., "10K", "Marathon").
    """
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
    main("M", "2024", "10K")