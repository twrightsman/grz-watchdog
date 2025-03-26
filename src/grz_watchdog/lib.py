from os import PathLike
from pathlib import Path

from sqlmodel import SQLModel, Field, Session, create_engine, select


class MetadataRecord(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    bucket: str
    key: str
    state: str


class MetadataDb:
    def __init__(self, db_path: PathLike):
        self._db_path = db_path
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine("sqlite:///{}".format(db_path))
        SQLModel.metadata.create_all(self.engine)

    def get_state(self, bucket: str, key: str) -> str | None:
        print(f"Getting state for {bucket}/{key}")
        with Session(self.engine) as session:
            statement = select(MetadataRecord).where(
                MetadataRecord.bucket == bucket and MetadataRecord.key == key
            )
            record = session.exec(statement).first()
            if record:
                return record.state
            else:
                return None

    def update_state(self, bucket: str, key: str, state: str):
        print(f"Updating state for {bucket}/{key} to {state}")
        with Session(self.engine) as session:
            session.add(MetadataRecord(bucket=bucket, key=key, state=state))

    def records(self, bucket: str) -> list[MetadataRecord]:
        print(f"Getting records for {bucket}")
        with Session(self.engine) as session:
            statement = select(MetadataRecord).where(MetadataRecord.bucket == bucket)
            records = session.exec(statement).all()
            return records
