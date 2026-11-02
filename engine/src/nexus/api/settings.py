"""Load HTTP admission policy without granting the API a model transport."""

from pydantic_settings import BaseSettings, SettingsConfigDict

from nexus.settings import AgentRuntimeMode, EngineSettings


class _RuntimeSelection(BaseSettings):
    model_config = SettingsConfigDict(env_ignore_empty=True, hide_input_in_errors=True)
    agent_runtime_mode: AgentRuntimeMode | None = None


class _ResponsesAdmissionSettings(EngineSettings):
    @property
    def research_admission_mode(self) -> AgentRuntimeMode:
        return AgentRuntimeMode.RESPONSES


def load_api_settings() -> EngineSettings:
    if _RuntimeSelection().agent_runtime_mode is AgentRuntimeMode.RESPONSES:
        # Only the host loads the project key. The HTTP process admits work for
        # the host and has no Responses executor of its own.
        return _ResponsesAdmissionSettings(
            agent_runtime_mode=AgentRuntimeMode.FIXTURE, openai_api_key=None
        )
    return EngineSettings()
