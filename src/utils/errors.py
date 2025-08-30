class DownloadError(Exception):
    pass


class ParseError(Exception):
    pass


class ValidationError(Exception):
    pass


def write_quarantine(target_path: str, reason: str) -> None:
    """
    Record a quarantine entry for a target resource with an explanatory reason.
    
    Intended behavior: move the file or dataset rows at `target_path` into a quarantine location
    (e.g., data/quarantine) and append a record describing `reason` to a persistent log or CSV.
    Currently this function is a placeholder and performs no actions.
    
    Parameters:
        target_path (str): Path to the file or dataset to be quarantined.
        reason (str): Brief explanation for why the resource is being quarantined.
    """
    # TODO: implement move to data/quarantine and append a reason log/CSV
    return
