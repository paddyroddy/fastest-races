from fastest_races._vars import HOUR_SECONDS, MINUTE_SECONDS


def format_seconds_to_display(total_seconds: int) -> str:
    """
    Format total seconds.

    Format total seconds into MM:SS if less than an hour, or H:MM:SS if an hour
    or more. This is used for the 'Fastest' column.
    """
    if total_seconds < HOUR_SECONDS:
        minutes = int(total_seconds // MINUTE_SECONDS)
        seconds = int(total_seconds % MINUTE_SECONDS)
        return f"{minutes:02d}:{seconds:02d}"
    # 1 hour or more
    hours = int(total_seconds // HOUR_SECONDS)
    minutes_after_hours = int(
        (total_seconds % HOUR_SECONDS)
        // MINUTE_SECONDS
    )
    seconds = int(total_seconds % MINUTE_SECONDS)
    # Using 'd' for hours allows single digit for 1-9 hours (e.g., 1:05:00)
    # Change to '{hours:02d}' if you always want two digits (e.g., 01:05:00)
    return f"{hours:d}:{minutes_after_hours:02d}:{seconds:02d}"


def format_threshold_minutes_to_display(threshold_min: int) -> str:
    """
    Format a minute threshold.

    Format a minute threshold into a readable string (e.g., '< 30', '< 1:00').
    Displays as <H:MM if threshold is 60 minutes or more, else <M. This is used
    for the column titles.
    """
    # If the threshold itself is 60 minutes or more
    if threshold_min >= MINUTE_SECONDS:
        threshold_hours = threshold_min // MINUTE_SECONDS
        threshold_remainder_minutes = (
            threshold_min % MINUTE_SECONDS
        )
        return f"< {threshold_hours:d}:{threshold_remainder_minutes:02d}"
    # For thresholds less than 60 minutes (e.g., < 30, < 45)
    return f"< {threshold_min:d}"
