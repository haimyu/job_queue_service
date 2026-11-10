"""Request payload models, validated at the API and again by the worker before a handler runs."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, EmailStr, Field, HttpUrl, model_validator

JobType = Literal["email", "webhook", "report", "batch"]
JobStatus = Literal[
    "scheduled", "pending", "processing", "completed", "failed", "cancelled"
]


class EmailPayload(BaseModel):
    to: EmailStr
    subject: str
    body: str


class WebhookPayload(BaseModel):
    url: HttpUrl
    body: dict | None = None


class ReportPayload(BaseModel):
    report_type: str
    params: dict = Field(default_factory=dict)


class BatchPayload(BaseModel):
    items: list = Field(min_length=1, max_length=1000)


PAYLOAD_MODELS: dict[str, type[BaseModel]] = {
    "email": EmailPayload,
    "webhook": WebhookPayload,
    "report": ReportPayload,
    "batch": BatchPayload,
}


class JobCreate(BaseModel):
    type: JobType
    payload: dict
    # Must carry a timezone, otherwise "10:00" would be ambiguous. Whether it is in the
    # future is decided by Postgres now() at insert time, not by Python.
    run_at: AwareDatetime | None = None
    priority: int = Field(default=0, ge=0, le=100)
    max_attempts: int = Field(default=3, ge=1, le=10)

    @model_validator(mode="after")
    def payload_matches_type(self) -> JobCreate:
        # A pydantic ValidationError raised here is reported by FastAPI as HTTP 422.
        PAYLOAD_MODELS[self.type].model_validate(self.payload)
        return self
