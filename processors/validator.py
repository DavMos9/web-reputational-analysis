"""
Validator
Verifica che ogni record rispetti il data contract (§6):
- source_type presente e non vuoto
- title presente e non vuoto
- url presente e non vuoto
- retrieved_at presente e non vuoto

I record non validi vengono scartati con un messaggio di log.
"""

from dataclasses import dataclass


# Campi obbligatori secondo il data contract
REQUIRED_FIELDS = ("source_type", "title", "url", "retrieved_at")


@dataclass
class ValidationResult:
    valid: list[dict]
    invalid: list[dict]
    errors: list[str]


def validate(record: dict) -> tuple[bool, str]:
    """
    Verifica un singolo record.

    Returns:
        (is_valid, error_message) — error_message è "" se valido.
    """
    for field in REQUIRED_FIELDS:
        value = record.get(field)
        if not value or (isinstance(value, str) and not value.strip()):
            return False, f"Campo obbligatorio mancante o vuoto: '{field}'"
    return True, ""


def validate_all(records: list[dict]) -> ValidationResult:
    """
    Valida una lista di record.

    Returns:
        ValidationResult con record validi, record scartati e messaggi di errore.
    """
    valid = []
    invalid = []
    errors = []

    for i, record in enumerate(records):
        is_valid, error_msg = validate(record)
        if is_valid:
            valid.append(record)
        else:
            invalid.append(record)
            # Includi abbastanza contesto per il debug
            context = f"[Record #{i}] fonte={record.get('source_type', '?')} | " \
                      f"query='{record.get('query', '?')}' | errore: {error_msg}"
            errors.append(context)

    return ValidationResult(valid=valid, invalid=invalid, errors=errors)
