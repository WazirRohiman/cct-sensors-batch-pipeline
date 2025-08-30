class DownloadError(Exception):
    pass


class ParseError(Exception):
    pass


class ValidationError(Exception):
    pass


def write_quarantine(target_path: str, reason: str) -> None:
    """Record a quarantine entry (could move file/rows and log reason)."""
    # TODO: implement move to data/quarantine and append a reason log/CSV
    return
