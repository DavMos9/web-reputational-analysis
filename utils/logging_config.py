"""
utils/logging_config.py

Configurazione centralizzata del logging per la pipeline.

Uso:
    from utils.logging_config import configure_logging
    configure_logging()                  # INFO, formato standard
    configure_logging(level=logging.DEBUG)  # verbose

Motivo dell'esistenza:
    Centralizzare basicConfig e il silenziamento dei logger di terze parti
    evita che main.py sia l'unico entry point che configura il logging.
    Qualsiasi script, notebook o test può chiamare configure_logging()
    senza reimplementare la stessa logica.
"""

from __future__ import annotations

import logging

# Logger di terze parti da abbassare a WARNING.
# Emettono messaggi INFO non utili all'utente (progress bar, load report, ecc.)
_NOISY_LOGGERS: tuple[str, ...] = (
    "wikipediaapi",
    "httpx",
    "transformers",
    "transformers.modeling_utils",
    "huggingface_hub",
    "huggingface_hub.repocard",
    "filelock",
    "urllib3",
)

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configura il logging dell'applicazione.

    Imposta il livello e il formato sul root logger, poi abbassa a WARNING
    i logger di librerie terze che emettono messaggi irrilevanti a INFO.

    Sicuro da chiamare più volte: basicConfig è no-op se il root logger
    ha già handler registrati.

    Args:
        level: livello di logging per il root logger (default: logging.INFO).
               Usare logging.DEBUG per output verbose durante lo sviluppo.
    """
    logging.basicConfig(
        level=level,
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
    )

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
