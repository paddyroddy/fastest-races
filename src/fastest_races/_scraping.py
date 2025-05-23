import bs4
import urllib3

from fastest_races._vars import ERROR_CODES


def get_html_table(gender: str, year: str, distance: str) -> bs4.element.Tag:
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
    url = (
        "https://www.thepowerof10.info/rankings/rankinglist.aspx?event="
        f"{distance}&agegroup=ALL&sex={gender}&year={year}"
    )

    http = urllib3.PoolManager()
    try:
        response = http.request("GET", url)

        if response.status >= ERROR_CODES:
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

    return table
