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
    response = urllib3.request("GET", url)
    soup = BeautifulSoup(response.text, "lxml")
    table = soup.find("span", {"id": "cphBody_lblCachedRankingList"}).find("table")



if __name__ == "__main__":
    url = "https://www.thepowerof10.info/rankings/rankinglist.aspx?event=10K&agegroup=ALL&sex=M&year=2024"
    main(url)
