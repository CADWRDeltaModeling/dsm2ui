"""Shared logging configuration for dsm2ui CLI commands."""
import logging


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure root logging for a CLI command.

    Sets up a simple timestamped console handler and silences noisy
    third-party loggers that produce spurious output at INFO/DEBUG level.

    Parameters
    ----------
    log_level:
        Root log level string (e.g. ``"INFO"``, ``"DEBUG"``, ``"WARNING"``).

    Returns
    -------
    logging.Logger
        The ``dsm2ui`` package logger.
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # pyhecdss has a broken logging.debug(..., (RuntimeWarning,)) call that
    # crashes Python's log formatter when DEBUG is enabled.
    logging.getLogger("pyhecdss").setLevel(logging.WARNING)
    # matplotlib font_manager is extremely verbose at DEBUG level.
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    # bokeh startup messages are not useful in CLI context.
    logging.getLogger("bokeh").setLevel(logging.WARNING)
    return logging.getLogger("dsm2ui")
