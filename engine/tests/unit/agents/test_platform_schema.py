"""API wire compatibility must not weaken the SEC monitoring contract."""

import hashlib
import json
from dataclasses import replace

import httpx
import pytest
from jsonschema import Draft202012Validator
from nexus.agents.codex_sdk import codex_output_schema
from nexus.agents.platform_wire import PlatformProfile, _hash, _wire
from nexus.agents.research_stages.http_lifecycle import HttpStageFailure
from nexus.monitoring.contracts import DisclosureReviewCondition
from pydantic import AnyUrl, BaseModel, ValidationError
from unit.agents.test_http_stage_lifecycle import Body, Protocol, terminal
from unit.agents.test_platform_http_stage_lifecycle import call
from unit.agents.test_platform_http_stage_lifecycle import credential as make_credential
from unit.agents.test_platform_http_stage_lifecycle import frozen as make_wire


class DisclosureOutput(BaseModel):
    condition: DisclosureReviewCondition


def encoded_schema(model):
    return json.loads(
        _wire({}, model, output_remaining=8192, profile=PlatformProfile(), forbidden=())
    )["text"]["format"]["schema"]


def test_api_encodes_fixed_sec_pair_without_changing_sdk_or_business_validation():
    sdk_before = codex_output_schema(DisclosureOutput)
    schema = encoded_schema(DisclosureOutput)
    forms = schema["$defs"]["DisclosureReviewCondition"]["properties"]["forms"]
    assert "prefixItems" not in forms
    assert forms["items"] == {"type": "string", "enum": ["8-K", "8-K/A"]}
    assert forms["minItems"] == forms["maxItems"] == 2
    validator = Draft202012Validator(forms)
    assert validator.is_valid(["8-K", "8-K/A"])
    for invalid in ([], ["8-K"], ["10-K", "8-K/A"], ["8-K", "8-K/A", "8-K"]):
        assert not validator.is_valid(invalid)
    assert codex_output_schema(DisclosureOutput) == sdk_before
    assert "prefixItems" in sdk_before["$defs"]["DisclosureReviewCondition"]["properties"]["forms"]
    condition = dict(
        signal="sec.issuer_disclosure_added",
        operator="exists_new",
        threshold=True,
        unit="filing",
        purpose="review",
        source="sec",
        forms=["8-K", "8-K/A"],
        max_age_seconds=300,
        time_authority="frozen_index_vs_local_receipt",
    )
    assert DisclosureOutput.model_validate({"condition": condition}).condition.forms == (
        "8-K",
        "8-K/A",
    )
    for invalid in (
        [],
        ["8-K"],
        ["8-K/A", "8-K"],
        ["8-K", "8-K"],
        ["10-K", "8-K/A"],
        ["8-K", "8-K/A", "8-K"],
    ):
        with pytest.raises(ValidationError):
            DisclosureOutput.model_validate({"condition": {**condition, "forms": invalid}})


def test_other_positional_arrays_are_rejected_before_wire_is_returned():
    class Unsupported(BaseModel):
        pair: tuple[int, str]

    with pytest.raises(ValueError, match="^PLATFORM_OUTPUT_SCHEMA_UNSUPPORTED$"):
        encoded_schema(Unsupported)


def test_open_ended_objects_are_rejected_before_wire_is_returned():
    class Unsupported(BaseModel):
        mapping: dict[str, int]

    with pytest.raises(ValueError, match="^PLATFORM_OUTPUT_SCHEMA_UNSUPPORTED$"):
        encoded_schema(Unsupported)


def test_unsupported_composition_is_not_silently_dropped():
    class Unsupported(BaseModel):
        value: str
        model_config = {"json_schema_extra": {"not": {"const": "sentinel"}}}

    with pytest.raises(ValueError, match="^PLATFORM_OUTPUT_SCHEMA_UNSUPPORTED$"):
        encoded_schema(Unsupported)


def test_unsupported_format_value_is_rejected_before_wire_is_returned():
    class Unsupported(BaseModel):
        link: AnyUrl

    with pytest.raises(ValueError, match="^PLATFORM_OUTPUT_SCHEMA_UNSUPPORTED$"):
        encoded_schema(Unsupported)


def test_property_names_are_not_confused_with_schema_keywords():
    class Allowed(BaseModel):
        prefixItems: str
        default: str

    schema = encoded_schema(Allowed)
    assert set(schema["properties"]) == {"prefixItems", "default"}


@pytest.mark.asyncio
@pytest.mark.parametrize("forms,valid", [(["8-K", "8-K/A"], True), (["8-K/A", "8-K"], False)])
async def test_real_http_parser_retains_original_position_check_and_cleanup(forms, valid):
    credential, frozen = make_credential.__wrapped__(), make_wire.__wrapped__()
    wire = _wire(
        {}, DisclosureOutput, output_remaining=8192, profile=PlatformProfile(), forbidden=()
    )
    digest = hashlib.sha256(wire).hexdigest()
    frozen = replace(
        frozen,
        wire=wire,
        body_sha256=digest,
        binding_hash=_hash(
            dict(allocation_hash=frozen.allocation_hash, stage="research", body_sha256=digest)
        ),
        input_units=len(wire) + 8192,
        input_upper_bound=len(wire) + 8192,
    )
    answer = {
        "condition": dict(
            signal="sec.issuer_disclosure_added",
            operator="exists_new",
            threshold=True,
            unit="filing",
            purpose="review",
            source="sec",
            forms=forms,
            max_age_seconds=300,
            time_authority="frozen_index_vs_local_receipt",
        )
    }
    # Both are in the transport subset; only the correctly ordered pair is
    # valid under the unchanged original business model at the receive edge.
    Draft202012Validator(json.loads(wire)["text"]["format"]["schema"]).validate(answer)
    event = terminal()
    event["response"]["output"][0]["content"][0]["text"] = json.dumps(answer)
    body = Body(("data: " + json.dumps(event) + "\n\n").encode())
    requests = []

    def handle(request):
        requests.append(request.content)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=body)

    async def check():
        return credential

    protocol = Protocol()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    if valid:
        await call(protocol, client, frozen, credential, check, output_model=DisclosureOutput)
    else:
        with pytest.raises(HttpStageFailure) as raised:
            await call(protocol, client, frozen, credential, check, output_model=DisclosureOutput)
        assert isinstance(raised.value.primary, ValidationError)
    assert requests == [wire]
    observations = [x for kind, x in protocol.events if kind == "observe"]
    assert len(observations) == 1 and observations[0]["valid"] is valid
    assert client.is_closed and body.closed == 1


def test_normal_preparation_rejects_unsupported_schema_without_writing_an_artifact(
    tmp_path, monkeypatch
):
    from uuid import uuid4

    from nexus.agents import platform_schema
    from nexus.agents.comparison_manifest import PLATFORM_THIRD_PURPOSE
    from nexus.ops.research_comparison import prepare_file
    from unit.agents.test_comparison_inputs import archive_source

    original = platform_schema.codex_output_schema

    def invalid(model):
        return {**original(model), "not": {"const": "unsupported"}}

    monkeypatch.setattr(platform_schema, "codex_output_schema", invalid)
    source = tmp_path / "source.json"
    data = json.dumps(archive_source()).encode()
    source.write_bytes(data)
    output = tmp_path / "prepared.json"
    with pytest.raises(ValueError, match="^PLATFORM_OUTPUT_SCHEMA_UNSUPPORTED$"):
        prepare_file(
            source=source,
            expected_source_hash=hashlib.sha256(data).hexdigest(),
            experiment_id=uuid4(),
            output=output,
            purpose=PLATFORM_THIRD_PURPOSE,
        )
    assert not output.exists()
