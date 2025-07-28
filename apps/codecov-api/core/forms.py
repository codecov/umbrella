import inspect
import json

from django import forms
from django.core.exceptions import ValidationError

from services.task.task import TaskService

task_service = TaskService()


class TaskServiceSubmissionForm(forms.Form):
    def _get_task_info(self):
        task_choices = [("", "-- Select a task method --")]
        task_info = {}

        for method_name in dir(task_service):
            if method_name.startswith("_"):
                continue
            if method_name in ["schedule_task"]:
                continue
            if method_name.endswith("_signature"):
                continue

            method = getattr(task_service, method_name)
            if not callable(method):
                continue

            try:
                sig = inspect.signature(method)
                parameters = []
                required_params = []
                optional_params = []

                for name, param in sig.parameters.items():
                    if name == "self":
                        continue

                    param_info = {
                        "name": name,
                        "type": str(param.annotation)
                        if param.annotation != param.empty
                        else "Any",
                        "default": str(param.default)
                        if param.default != param.empty
                        else None,
                        "required": param.default == param.empty,
                    }

                    parameters.append(param_info)

                    if param.default == param.empty:
                        required_params.append(name)
                    else:
                        optional_params.append(name)

                task_info[method_name] = {
                    "description": f"TaskService.{method_name}",
                    "signature": str(sig),
                    "parameters": parameters,
                    "required": required_params,
                    "optional": optional_params,
                }
            except Exception:
                continue

        task_choices.extend(
            [(method_name, method_name) for method_name in sorted(task_info.keys())]
        )

        return {"choices": task_choices, "info": task_info}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        task_info = self._get_task_info()

        self.fields["task_method"] = forms.ChoiceField(
            label="Select Task Method",
            help_text="Choose a TaskService method to execute",
            required=True,
            choices=task_info["choices"],
            widget=forms.Select(
                attrs={
                    "class": "vTextField",
                    "style": "width: 100%;",
                    "onchange": "updateTaskPreview(this.value)",
                }
            ),
        )

        self.fields["task_preview"] = forms.CharField(
            label="Method Signature & Parameters",
            help_text="Function signature and parameters for the selected method",
            required=False,
            widget=forms.Textarea(
                attrs={
                    "class": "vLargeTextField",
                    "rows": 8,
                    "cols": 80,
                    "readonly": "readonly",
                    "style": "width: 100%; font-family: monospace; background-color: #1e1e1e; color: #d4d4d4; border: 1px solid #3c3c3c;",
                    "id": "task-preview-field",
                }
            ),
            initial="Select a task method to see its signature and parameters",
        )

        self.fields["method_kwargs"] = forms.CharField(
            label="Method Arguments",
            help_text="JSON object with method keyword arguments (e.g., {'repoid': 1, 'commitid': 'abc123'})",
            widget=forms.Textarea(
                attrs={
                    "class": "vLargeTextField",
                    "rows": 12,
                    "cols": 80,
                    "placeholder": '{\n  "repoid": 1,\n  "commitid": "abc123"\n}',
                    "style": "width: 100%; font-family: monospace;",
                }
            ),
            initial="{}",
        )

    def clean(self):
        cleaned_data = super().clean()
        if not cleaned_data:
            return cleaned_data
        task_method = cleaned_data.get("task_method")

        if not task_method:
            raise ValidationError("Please select a task method.")

        return cleaned_data

    def clean_method_kwargs(self):
        if not self.cleaned_data:
            return {}
        method_kwargs = self.cleaned_data.get("method_kwargs", "{}")
        try:
            parsed = json.loads(method_kwargs)
            if not isinstance(parsed, dict):
                raise ValidationError(
                    "Method arguments must be a JSON object (dictionary)."
                )
            return parsed
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON format: {e}")

    def get_task_parameter_info_json(self):
        return json.dumps(self._get_task_info()["info"])

    def call_task_method(self):
        task_method = self.cleaned_data.get("task_method")
        method_kwargs = self.cleaned_data.get("method_kwargs", {})

        if not task_method:
            raise ValidationError("No task method selected")

        try:
            method = getattr(task_service, task_method)

            if not callable(method):
                raise ValidationError(f"Method '{task_method}' is not callable")

            result = method(**method_kwargs)
            return result

        except ImportError:
            raise ValidationError("Cannot import TaskService")
        except AttributeError:
            raise ValidationError(f"Method '{task_method}' not found on TaskService")
        except TypeError as e:
            raise ValidationError(
                f"Invalid arguments for method '{task_method}': {str(e)}"
            )
