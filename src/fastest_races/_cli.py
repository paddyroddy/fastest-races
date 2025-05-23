import argparse
import logging

import fastest_races._calculations
import fastest_races._report
import fastest_races._scraping

_logger = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and analyse athlete performance data from The Power of 10."
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
    return parser.parse_args()


def perform_analysis(args: argparse.Namespace) -> None:
    msg = f"Fetching data for {args.gender} {args.distance} in {args.year}..."
    _logger.info(msg)
    try:
        rankings = fastest_races._calculations.get_ranking_data(
            args.gender, args.year, args.distance
        )

        if rankings.empty:
            msg = (
                f"No valid performance data found for {args.gender} "
                f"{args.distance} in {args.year}. This might be due to a "
                "mismatch in `Perf` format, no data available, or an issue "
                "with the website's structure for this query."
            )
            _logger.error(msg)
            return

        output_df = fastest_races._calculations.calculate_performance_metrics(rankings)

        if output_df.empty:
            msg = (
                "No aggregated performance metrics could be calculated for "
                f"{args.gender} {args.distance} in {args.year}. Output "
                "DataFrame is empty."
            )
            _logger.error(msg)
            return

        fastest_races._report.generate_and_open_html_report(output_df)

    except (ConnectionError, ValueError) as e:
        msg = f"Error: {e}"
        _logger.exception(msg)
    except Exception as e:
        msg = f"An unexpected error occurred: {e}"
        _logger.exception(msg)
