from uuid import UUID, uuid4

import pytest
from nexus.paper.candidate_batch import CandidateBatchInput

AAPL_ID = UUID("11111111-1111-1111-1111-111111111111")
MSFT_ID = UUID("22222222-2222-2222-2222-222222222222")


def test_candidate_batch_input_sorts_and_rejects_duplicates() -> None:
    with pytest.raises(ValueError, match="CANDIDATE_BATCH_INVALID"):
        CandidateBatchInput(instrument_ids=(AAPL_ID, AAPL_ID))

    value = CandidateBatchInput(instrument_ids=(MSFT_ID, AAPL_ID))

    assert value.instrument_ids == (AAPL_ID, MSFT_ID)


@pytest.mark.parametrize("count", [0, 6])
def test_candidate_batch_input_rejects_out_of_bounds_scope(count: int) -> None:
    with pytest.raises(ValueError, match="CANDIDATE_BATCH_INVALID"):
        CandidateBatchInput(instrument_ids=tuple(uuid4() for _ in range(count)))


def test_candidate_batch_input_rejects_ticker_text() -> None:
    with pytest.raises(ValueError, match="CANDIDATE_BATCH_INVALID"):
        CandidateBatchInput(instrument_ids=("AAPL",))  # type: ignore[arg-type]
