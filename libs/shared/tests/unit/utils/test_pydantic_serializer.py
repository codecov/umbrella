import importlib
from typing import Any, Literal
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field

from shared.utils.pydantic_serializer import (
    PydanticModelDump,
    load_from_path,
    register_preserializer,
)


class TestLoadFromPath:
    """Tests for the load_from_path function."""

    def test_load_simple_class(self):
        """Test loading a simple class from a module."""
        # Load the BaseModel class from pydantic
        result = load_from_path("pydantic", "BaseModel")
        assert result is BaseModel

    def test_load_function_from_module(self):
        """Test loading a function from a module."""
        # Load the import_module function from importlib
        result = load_from_path("importlib", "import_module")
        assert result is importlib.import_module

    def test_load_nested_attribute(self):
        """Test loading a nested attribute (e.g., class.method)."""
        # Load a nested attribute
        result = load_from_path("pydantic", "BaseModel.model_dump")
        assert result is BaseModel.model_dump

    def test_load_nonexistent_module(self):
        """Test loading from a non-existent module raises ImportError."""
        with pytest.raises(ImportError):
            load_from_path("nonexistent_module", "SomeClass")

    def test_load_nonexistent_attribute(self):
        """Test loading a non-existent attribute raises AttributeError."""
        with pytest.raises(AttributeError):
            load_from_path("pydantic", "NonExistentClass")


class TestPydanticModelDump:
    """Tests for the PydanticModelDump class."""

    def setup_method(self):
        """Set up test fixtures."""

        class TestModel(BaseModel):
            name: str
            age: int = Field(default=25)
            active: bool = True

        class NestedModel(BaseModel):
            test_model: TestModel
            description: str

        self.TestModel = TestModel
        self.NestedModel = NestedModel

    def test_compatible_with_valid_model(self):
        """Test compatible_with returns True for valid BaseModel subclass."""
        result = PydanticModelDump.compatible_with(self.TestModel)
        assert result is True

    def test_compatible_with_invalid_type(self):
        """Test compatible_with raises TypeError for non-BaseModel type."""
        with pytest.raises(TypeError) as exc_info:
            PydanticModelDump.compatible_with(dict)

        assert "PydanticModelDump requires a type that inherits from BaseModel" in str(
            exc_info.value
        )

    def test_compatible_with_non_type(self):
        """Test compatible_with raises TypeError for non-type object."""
        with pytest.raises(TypeError):
            PydanticModelDump.compatible_with("not a type")

    def test_pack_simple_model(self):
        """Test packing a simple Pydantic model."""
        model = self.TestModel(name="John", age=30, active=False)
        result = PydanticModelDump.pack(model)

        expected = {
            "module": model.__class__.__module__,
            "qualname": model.__class__.__qualname__,
            "dump": {"name": "John", "age": 30, "active": False},
        }
        assert result == expected

    def test_pack_model_with_defaults(self):
        """Test packing a model with default values."""
        model = self.TestModel(name="Jane")
        result = PydanticModelDump.pack(model)

        expected = {
            "module": model.__class__.__module__,
            "qualname": model.__class__.__qualname__,
            "dump": {"name": "Jane", "age": 25, "active": True},
        }
        assert result == expected

    def test_pack_nested_model(self):
        """Test packing a nested Pydantic model."""
        inner_model = self.TestModel(name="Inner", age=20)
        nested_model = self.NestedModel(test_model=inner_model, description="Test")

        result = PydanticModelDump.pack(nested_model)

        expected_dump = {
            "test_model": {"name": "Inner", "age": 20, "active": True},
            "description": "Test",
        }

        assert result["module"] == nested_model.__class__.__module__
        assert result["qualname"] == nested_model.__class__.__qualname__
        assert result["dump"] == expected_dump

    @patch("shared.utils.pydantic_serializer.load_from_path")
    def test_unpack_simple_model(self, mock_load):
        """Test unpacking a simple model."""
        mock_load.return_value = self.TestModel

        data = {
            "module": "test_module",
            "qualname": "TestModel",
            "dump": {"name": "John", "age": 30, "active": False},
        }

        result = PydanticModelDump.unpack(data)

        mock_load.assert_called_once_with("test_module", "TestModel")
        assert isinstance(result, self.TestModel)
        assert result.name == "John"
        assert result.age == 30
        assert result.active is False

    @patch("shared.utils.pydantic_serializer.load_from_path")
    def test_unpack_nested_model(self, mock_load):
        """Test unpacking a nested model."""
        mock_load.return_value = self.NestedModel

        data = {
            "module": "test_module",
            "qualname": "NestedModel",
            "dump": {
                "test_model": {"name": "Inner", "age": 20, "active": True},
                "description": "Test",
            },
        }

        result = PydanticModelDump.unpack(data)

        mock_load.assert_called_once_with("test_module", "NestedModel")
        assert isinstance(result, self.NestedModel)
        assert result.description == "Test"
        assert isinstance(result.test_model, self.TestModel)
        assert result.test_model.name == "Inner"
        assert result.test_model.age == 20
        assert result.test_model.active is True

    @patch("shared.utils.pydantic_serializer.load_from_path")
    def test_unpack_invalid_type(self, mock_load):
        """Test unpacking raises TypeError for non-BaseModel type."""
        mock_load.return_value = dict

        data = {"module": "builtins", "qualname": "dict", "dump": {"key": "value"}}

        with pytest.raises(TypeError) as exc_info:
            PydanticModelDump.unpack(data)

        assert "Cannot unpack" in str(exc_info.value)
        assert "not a Pydantic model" in str(exc_info.value)

    @patch("shared.utils.pydantic_serializer.load_from_path")
    def test_unpack_non_type(self, mock_load):
        """Test unpacking raises TypeError for non-type object."""
        mock_load.return_value = "not a type"

        data = {
            "module": "some_module",
            "qualname": "some_object",
            "dump": {"key": "value"},
        }

        with pytest.raises(TypeError) as exc_info:
            PydanticModelDump.unpack(data)

        assert "Cannot unpack" in str(exc_info.value)
        assert "not a Pydantic model" in str(exc_info.value)


# Test models defined at module level to avoid <locals> in __qualname__
class TestModelForRegistration(BaseModel):
    __test__ = False
    name: str


class TestRegisterPreserializer:
    """Tests for the register_preserializer decorator."""

    def setup_method(self):
        """Set up test fixtures."""

        class MockPreserializer:
            @classmethod
            def compatible_with(cls, type_: type) -> Literal[True]:
                if not issubclass(type_, BaseModel):
                    raise TypeError("Not a BaseModel")
                return True

            @classmethod
            def pack(cls, obj: Any) -> Any:
                return {"packed": True}

            @classmethod
            def unpack(cls, data: Any) -> object:
                return {"unpacked": True}

        self.MockPreserializer = MockPreserializer
        self.TestModel = TestModelForRegistration

    @patch("shared.utils.pydantic_serializer.register_type")
    def test_register_valid_model(self, mock_register):
        """Test registering a valid BaseModel with preserializer."""
        decorator = register_preserializer(self.MockPreserializer())

        # Apply the decorator
        result = decorator(self.TestModel)

        # Check that the original class is returned
        assert result is self.TestModel

        # Check that register_type was called with correct parameters
        mock_register.assert_called_once_with(
            self.TestModel,
            f"{self.TestModel.__module__}.{self.TestModel.__qualname__}",
            encoder=self.MockPreserializer.pack,
            decoder=self.MockPreserializer.unpack,
        )

    def test_register_incompatible_type(self):
        """Test registering an incompatible type raises TypeError."""
        decorator = register_preserializer(self.MockPreserializer())

        with pytest.raises(TypeError) as exc_info:
            decorator(dict)

        assert "is not compatible with" in str(exc_info.value)

    def test_register_local_class(self):
        """Test registering a local class raises TypeError."""

        class LocalClass(BaseModel):
            value: str

        decorator = register_preserializer(self.MockPreserializer())

        with pytest.raises(TypeError) as exc_info:
            decorator(LocalClass)

        assert (
            "You cannot register preserializers on objects that are not directly accessible at import time"
            in str(exc_info.value)
        )

    @patch("shared.utils.pydantic_serializer.register_type")
    def test_register_main_module_class(self, mock_register):
        """Test registering a class from __main__ module raises TypeError."""
        # Create a mock class that appears to be from __main__
        mock_class = MagicMock()
        mock_class.__module__ = "__main__"
        mock_class.__qualname__ = "TestClass"

        decorator = register_preserializer(self.MockPreserializer())

        with pytest.raises(TypeError) as exc_info:
            decorator(mock_class)

        assert (
            "You cannot register preserializers on objects that are not directly accessible at import time"
            in str(exc_info.value)
        )

    def test_preserializer_compatibility_exception(self):
        """Test that exceptions from compatible_with are properly wrapped."""

        class FailingPreserializer:
            @classmethod
            def compatible_with(cls, type_: type) -> Literal[True]:
                raise ValueError("Custom error message")

            @classmethod
            def pack(cls, obj: Any) -> Any:
                return obj

            @classmethod
            def unpack(cls, data: Any) -> object:
                return data

        decorator = register_preserializer(FailingPreserializer())

        with pytest.raises(TypeError) as exc_info:
            decorator(self.TestModel)

        assert "is not compatible with" in str(exc_info.value)
        assert "Custom error message" in str(exc_info.value)


class TestPreserializerProtocol:
    """Tests for the Preserializer protocol."""

    def test_preserializer_protocol_implementation(self):
        """Test that a class implementing Preserializer protocol works correctly."""

        class ValidPreserializer:
            @classmethod
            def compatible_with(cls, type_: type) -> Literal[True]:
                return True

            @classmethod
            def pack(cls, obj: Any) -> Any:
                return {"packed": str(obj)}

            @classmethod
            def unpack(cls, data: Any) -> object:
                return data["packed"]

        # Test that the implementation satisfies the protocol
        preserializer = ValidPreserializer()

        # Test compatible_with
        assert preserializer.compatible_with(BaseModel) is True

        # Test pack
        result = preserializer.pack("test")
        assert result == {"packed": "test"}

        # Test unpack
        unpacked = preserializer.unpack({"packed": "test"})
        assert unpacked == "test"


class TestIntegration:
    """Integration tests combining multiple components."""

    def test_full_round_trip(self):
        """Test a complete pack/unpack cycle."""

        class TestModel(BaseModel):
            name: str
            age: int
            active: bool = True

        # Create a model instance
        original = TestModel(name="Alice", age=28, active=False)

        # Pack it
        packed = PydanticModelDump.pack(original)

        # Verify packed structure
        assert packed["module"] == original.__class__.__module__
        assert packed["qualname"] == original.__class__.__qualname__
        assert packed["dump"]["name"] == "Alice"
        assert packed["dump"]["age"] == 28
        assert packed["dump"]["active"] is False

        # Unpack it
        with patch("shared.utils.pydantic_serializer.load_from_path") as mock_load:
            mock_load.return_value = TestModel
            unpacked = PydanticModelDump.unpack(packed)

        # Verify unpacked model
        assert isinstance(unpacked, TestModel)
        assert unpacked.name == "Alice"
        assert unpacked.age == 28
        assert unpacked.active is False

        # Verify they're equivalent (but not the same object)
        assert unpacked == original
        assert unpacked is not original
