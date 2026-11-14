"""Strict typed primary-oracle facts; absence is never a successful observation."""
from dataclasses import dataclass


class FactError(ValueError):
    pass


def integer(value, name):
    if type(value) is not int or value < 0:
        raise FactError(f"{name}: expected nonnegative integer")
    return value


def text(value, name):
    if not isinstance(value, str) or not value:
        raise FactError(f"{name}: expected nonempty identity")
    return value


def generation(value):
    if type(value) is int:
        return integer(value, "generation")
    return text(value, "generation")


def boolean(value, name):
    if type(value) is not bool:
        raise FactError(f"{name}: expected boolean")
    return value


@dataclass(frozen=True)
class Endpoint:
    id: str
    generation: int | str
    created_ns: int

    @classmethod
    def parse(cls, data):
        return cls(text(data["id"], "endpoint.id"), generation(data["generation"]), integer(data["created_ns"], "endpoint.created_ns"))


@dataclass(frozen=True)
class Delivery:
    sequence: int
    at_ns: int
    writer_id: str
    writer_generation: int | str
    reader_id: str | None = None
    reader_generation: int | str | None = None

    @classmethod
    def parse(cls, data, received=False):
        return cls(integer(data["sequence"], "sequence"), integer(data["at_ns"], "delivery.at_ns"),
                   text(data["writer_id"], "delivery.writer_id"),
                   generation(data["writer_generation" if received else "generation"]),
                   text(data["reader_id"], "delivery.reader_id") if received else None,
                   generation(data["reader_generation"]) if received else None)


@dataclass(frozen=True)
class PrimaryFacts:
    writer: Endpoint
    reader: Endpoint
    matched_writer: str
    matched_reader: str
    matched_ns: int
    window_start_ns: int
    window_end_ns: int
    window_complete: bool
    sent: tuple[Delivery, ...]
    received: tuple[Delivery, ...]

    @classmethod
    def parse(cls, data):
        writer, reader = Endpoint.parse(data["writer"]), Endpoint.parse(data["reader"])
        match, window = data["match"], data["window"]
        if not isinstance(match, dict):
            raise FactError("match_not_observed")
        if not isinstance(data["sent"], list) or not isinstance(data["received"], list):
            raise FactError("delivery collections must be lists")
        result = cls(writer, reader, text(match["writer_id"], "match.writer_id"),
                     text(match["reader_id"], "match.reader_id"), integer(match["at_ns"], "match.at_ns"),
                     integer(window["start_ns"], "window.start_ns"), integer(window["end_ns"], "window.end_ns"),
                     boolean(window["complete"], "window.complete"),
                     tuple(Delivery.parse(item) for item in data["sent"]),
                     tuple(Delivery.parse(item, True) for item in data["received"]))
        if result.window_end_ns <= result.window_start_ns:
            raise FactError("invalid observation window")
        if result.matched_ns < max(writer.created_ns, reader.created_ns):
            raise FactError("match timestamp precedes endpoint creation")
        if result.window_start_ns < result.matched_ns:
            raise FactError("window precedes match")
        if any(item.at_ns > result.window_end_ns for item in result.sent + result.received):
            raise FactError("delivery outside recorded observation boundary")
        return result
