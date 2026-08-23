import inspect

from nexus.diagnostics import repository, service


def test_production_modules_expose_no_report_shaped_writer() -> None:
    offenders = []
    for module in (repository, service):
        for name, function in inspect.getmembers(module, inspect.iscoroutinefunction):
            parameters = inspect.signature(function).parameters
            if "session" in parameters and "report" in parameters:
                offenders.append(f"{module.__name__}.{name}")

    assert offenders == []
