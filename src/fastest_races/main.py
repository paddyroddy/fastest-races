import pandas as pd
import bs4
import urllib3


def main(url: str) -> None:
    """_summary_.

    Parameters
    ----------
    url
        _description_.
    """
    http = urllib3.PoolManager()
    response = http.request("GET", url)
    html_content = response.data.decode("utf-8")
    soup = bs4.BeautifulSoup(html_content, "html.parser")
    table = soup.find("span", {"id": "cphBody_lblCachedRankingList"}).find("table")
    df = pd.read_html(str(table), header=1, index_col=0)[0]
    df = df.drop(
        [
            "Unnamed: 2",
            "Unnamed: 3",
            "PB",
            "Unnamed: 5",
            "Unnamed: 7",
            "Year",
            "Coach",
            "Club",
            "Unnamed: 13",
        ],
        axis=1,
    )
    df["Date"] = pd.to_datetime(df["Date"])
    df[["Venue", "Country"]] = df["Venue"].str.split(",", expand=True)
    df["County"] = df["Country"].fillna("UK")
    print()


if __name__ == "__main__":
    url = "https://www.thepowerof10.info/rankings/rankinglist.aspx?event=HepI&agegroup=ALL&sex=M&year=2024"
    main(url)
