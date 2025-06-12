from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
import os
from langchain_openai import ChatOpenAI

class Settings(BaseSettings):
    DATA_DIR: Path = Field(default=Path(__file__).parents[2] / "data")
    CSV_NAME: str = Field(default="transactions.csv")  # always saved as this
    AUTO_RELOAD: bool = True
    OPENAI_API_KEY: str = Field(default="")
    LLM_MODEL: str = Field(default="")

    class Config:
        env_file = ".env"

    @property
    def csv_path(self) -> Path:
        return self.DATA_DIR / self.CSV_NAME
    
    @property
    def llm(self):
        return ChatOpenAI(
            model=self.LLM_MODEL,
            temperature=0,
            api_key=self.OPENAI_API_KEY
        )

settings = Settings()
model = settings.llm
