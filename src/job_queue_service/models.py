"""Request payload models, validated at the API and again by the worker before a handler runs."""

from typing import Literal

from pydantic import BaseModel, EmailStr, Field, HttpUrl, model_validator


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
    type: Literal["email", "webhook", "report", "batch"]
    payload: dict
    priority: int = Field(default=0, ge=0, le=100)
    max_attempts: int = Field(default=3, ge=1, le=10)

    @model_validator(mode="after")
    def payload_matches_type(self) -> JobCreate:
        # A pydantic ValidationError raised here is reported by FastAPI as HTTP 422.
        PAYLOAD_MODELS[self.type].model_validate(self.payload)
        return self
