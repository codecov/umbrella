from importlib import import_module
from types import ModuleType
from typing import Any, Literal, Protocol, TypedDict, TypeVar

from kombu.utils.json import register_type
from pydantic import BaseModel

#############################################################################################
# This file provides a way to serialize and deserialize Pydantic models automatically for use
# with Celery tasks.
#
# Source: https://dosu.dev/blog/celery-preserializers-a-low-friction-path-to-pydantic-support
#
#############################################################################################

T = TypeVar("T")


def load_from_path(module: str, qualname: str) -> Any:
    """
    Given a dotted path to a module and the qualified name of a member of the
     module, import the module and return the named member.
    """
    m = import_module(module)
    o: type | ModuleType = m
    # this handles e.g. a class nested in a class
    for a in qualname.split("."):
        o = getattr(o, a)
    return o


class Preserializer(Protocol):
    """
    A Preserializer can be used to `pack` a non-serializable object into a
     serializable one, then `unpack` it again.
    """

    @classmethod
    def compatible_with(cls, type_: type) -> Literal[True]:
        """
        If the given type is compatible with this strategy, return `True`. If
         not, raise an exception explaining why it isn't.
        """

    @classmethod
    def pack(cls, obj: Any) -> Any:
        """
        Pack the given object into a JSON-serializable object.
        """

    @classmethod
    def unpack(cls, data: Any) -> object:
        """
        Unpack the serializable object back into an instance of its original
        type.
        """


class PackedModel(TypedDict):
    module: str
    qualname: str
    dump: dict[str, Any]


class PydanticModelDump:
    @classmethod
    def compatible_with(cls, type_: type) -> Literal[True]:
        if not issubclass(type_, BaseModel):
            raise TypeError(
                "PydanticModelDump requires a type that inherits from BaseModel"
            )
        return True

    @classmethod
    def pack(cls, obj: BaseModel) -> PackedModel:
        return {
            "module": obj.__class__.__module__,
            "qualname": obj.__class__.__qualname__,
            "dump": obj.model_dump(),
        }

    @classmethod
    def unpack(cls, data: PackedModel) -> BaseModel:
        t = load_from_path(data["module"], data["qualname"])
        if not (isinstance(t, type) and issubclass(t, BaseModel)):
            raise TypeError(f"Cannot unpack {t}: not a Pydantic model")
        # This works for basic models, but may need to be extended for more
        # complex ones
        return t(**data["dump"])


class register_preserializer:
    """
    Decorator factory that registers a Preserializer for the decorated type in
    the Kombu JSON type registry.
    """

    def __init__(self, preserializer: Preserializer):
        self.preserializer = preserializer

    def __call__(self, type_: type[T]) -> type[T]:
        if "<locals>" in type_.__qualname__ or "__main__" in type_.__module__:
            raise TypeError(
                "You cannot register preserializers on objects that are not "
                "directly accessible at import time."
            )

        try:
            self.preserializer.compatible_with(type_)
        except Exception as e:
            raise TypeError(
                f"{type_} is not compatible with {self.preserializer}: {e}"
            ) from e

        register_type(
            type_,
            f"{type_.__module__}.{type_.__qualname__}",
            encoder=self.preserializer.pack,
            decoder=self.preserializer.unpack,
        )
        return type_
