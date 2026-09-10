from nexus.api.schemas.diagnostics import PortfolioDiagnosticResponse


def test_diagnostic_response_exposes_optional_persisted_report_identity() -> None:
    field = PortfolioDiagnosticResponse.model_fields["diagnostic_report_id"]

    assert field.is_required()
    assert field.annotation is not None
