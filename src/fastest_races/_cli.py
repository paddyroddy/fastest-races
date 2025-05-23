import argparse


def get_args() -> argparse.Namespace:
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
    return parser.parse_args()
