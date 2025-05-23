import pandas as pd
import bs4
import urllib3
import io
import math

def main(gender: str, year: str, distance: str) -> None:
    """_summary_.

    Parameters
    ----------
    url
        _description_.
    """
    url = f"https://www.thepowerof10.info/rankings/rankinglist.aspx?event={distance}&agegroup=ALL&sex={gender}&year={year}"
    http = urllib3.PoolManager()
    response = http.request("GET", url)
    html_content = response.data.decode("utf-8")
    soup = bs4.BeautifulSoup(html_content, "html.parser")
    table = soup.find("span", {"id": "cphBody_lblCachedRankingList"}).find("table")
    df = pd.read_html(io.StringIO(str(table)), header=1, index_col=0)[0]
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    df = df.loc[df["Perf"].str.match(r"^\d+:\d{2}$")]
    df = df.sort_values("Perf").reset_index()
    df = df.drop(["Gun", "PB", "Name", "Coach", "Club", "Rank"], axis=1)
    df["Date"] = pd.to_datetime(df["Date"], format="%d %b %y")
    df[["Venue", "Country"]] = df["Venue"].str.split(",", expand=True)
    df["Country"] = df["Country"].fillna("UK")

    # Convert 'Perf' to seconds for numerical comparison
    df["Perf_seconds"] = df["Perf"].apply(
        lambda x: int(x.split(":")[0]) * 60 + int(x.split(":")[1])
    )
    
    # 1. Calculate Global Minimum and Maximum Performance in seconds
    global_min_seconds = df['Perf_seconds'].min()
    global_max_seconds = df['Perf_seconds'].max()

    # 2. Determine the dynamic minute thresholds for categories
    # The first category starts from the minute *after* the global minimum.
    # E.g., if min is 27:57 (1677s), 1677/60 = 27.95. floor(27.95) = 27. +1 makes it 28. So '< 28'.
    start_minute_threshold = math.floor(global_min_seconds / 60) + 1

    # The last category should go up to the minute that fully encompasses the slowest time.
    # E.g., if max is 34:40 (2080s), 2080/60 = 34.66. ceil(34.66) = 35. So '< 35'.
    # If max is 35:00 (2100s), 2100/60 = 35.0. ceil(35.0) = 35. So '< 35'.
    end_minute_threshold = math.ceil(global_max_seconds / 60)

    # Generate the list of minute thresholds.
    # `range` is exclusive of the stop value, so `end_minute_threshold + 1` is used to include the last threshold.
    dynamic_threshold_minutes = list(range(start_minute_threshold, end_minute_threshold + 1))

    # --- Custom aggregation function ---
    def aggregate_dynamic_thresholds(group):
        """
        Custom aggregation function applied to each group.
        It generates count columns based on the globally determined dynamic thresholds.
        """
        results = {}

        # Calculate counts for each global threshold
        for threshold_min in dynamic_threshold_minutes:
            col_name = f"< {threshold_min}" # Column name like "< 28", "< 29"
            threshold_seconds = threshold_min * 60
            results[col_name] = (group['Perf_seconds'] < threshold_seconds).sum()

        # Add the minimum performance for the current group
        results['min'] = pd.to_datetime(group['Perf_seconds'].min(), unit='s').strftime('%M:%S')

        # Return the results as a Pandas Series. This Series will become a row in the final DataFrame.
        return pd.Series(results)

    # --- Grouping and applying the aggregation ---
    grouped = df.groupby(["Date", "Venue", "Country"])
    output = grouped.apply(aggregate_dynamic_thresholds)

    # --- Final sorting and display (modified to sort by threshold columns) ---

    # Create a list of column names to sort by in the desired order
    sort_columns = [f"< {m}" for m in dynamic_threshold_minutes]

    # Add the 'min' column to the end of the sort order if you want to use it as a tie-breaker
    # (e.g., if two events have the same counts across all thresholds, sort by the fastest min time)
    sort_columns.append('min')


    # Sort by the dynamically generated threshold columns
    # For numerical counts, ascending=False will sort from highest count to lowest.
    # For 'min', if added, you'd usually want ascending=True (fastest times first).
    # If you mix ascending/descending, provide a list for `ascending` argument:
    # `ascending_order = [False] * len(dynamic_threshold_minutes) + [True]`

    output = output.sort_values(by=sort_columns, ascending=False) # Sorting by all thresholds descending

    print("Aggregated Output (sorted by dynamic threshold columns):\n")
    print(output.to_markdown(index=True, numalign="left", stralign="left"))


if __name__ == "__main__":
    main("M", "2024", "10K")
