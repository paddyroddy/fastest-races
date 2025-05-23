import logging
import os
import pathlib
import webbrowser

import pandas as pd

_logger = logging.getLogger(__name__)


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
