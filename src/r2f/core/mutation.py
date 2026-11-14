"""Execution mutation receipts, deliberately separate from input-layer receipts."""

from dataclasses import dataclass, fields


@dataclass
class MutationReceipt:
    planned: bool = False
    submitted: bool = False
    carrier_admitted: bool = False
    configured: bool = False
    applied: bool = False
    hit: bool = False
    effect_observed: bool = False

    def validate(self):
        previous = True
        for name in self.field_names():
            value = getattr(self, name)
            if type(value) is not bool:
                raise ValueError(f"mutation_receipt.{name} must be a boolean")
            if value and not previous:
                raise ValueError(f"mutation_receipt.{name} requires all preceding receipt flags")
            previous = value
        return self

    def to_dict(self):
        self.validate()
        return {name: getattr(self, name) for name in self.field_names()}

    @staticmethod
    def field_names():
        return tuple(f.name for f in fields(MutationReceipt))

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or set(data) != set(cls.field_names()):
            raise ValueError("mutation_receipt must contain exactly the seven nested boolean fields")
        return cls(**data).validate()

    @classmethod
    def from_record(cls, record):
        if not isinstance(record, dict) or any(name in record for name in cls.field_names()):
            raise ValueError("flat or mixed mutation receipt fields are prohibited")
        return cls.from_dict(record.get("mutation_receipt"))
