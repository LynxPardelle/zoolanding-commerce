"""Provider-neutral offer primitives."""

from dataclasses import dataclass, field
import re


@dataclass(frozen=True, slots=True)
class Money:
    amount_minor: int
    currency: str
    supported_currencies: frozenset[str] = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.amount_minor) is not int or self.amount_minor < 0:
            raise ValueError("amount_minor must be a non-negative integer")
        if not isinstance(self.currency, str) or re.fullmatch(r"[A-Z]{3}", self.currency, re.ASCII) is None:
            raise ValueError("currency must be a canonical three-letter code")
        if type(self.supported_currencies) is not frozenset or not self.supported_currencies:
            raise ValueError("supported_currencies must be a non-empty frozenset")
        if any(
            not isinstance(code, str) or re.fullmatch(r"[A-Z]{3}", code, re.ASCII) is None
            for code in self.supported_currencies
        ):
            raise ValueError("supported_currencies must contain canonical three-letter codes")
        if self.currency not in self.supported_currencies:
            raise ValueError("currency is not enabled by the owning policy")
