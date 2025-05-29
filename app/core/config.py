from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    DATA_DIR: Path = Field(default=Path(__file__).parents[2] / "data")
    CSV_NAME: str = Field(default="transactions.csv")  # always saved as this
    AUTO_RELOAD: bool = True

    class Config:
        env_file = ".env"

    @property
    def csv_path(self) -> Path:
        return self.DATA_DIR / self.CSV_NAME

settings = Settings()
