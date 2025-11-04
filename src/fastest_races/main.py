import fastest_races._cli


def main() -> None:
    """Parse arguments, fetch, process, and display athlete performance data."""
    args = fastest_races._cli.get_args()
    fastest_races._cli.perform_analysis(args)


if __name__ == "__main__":
    main()
