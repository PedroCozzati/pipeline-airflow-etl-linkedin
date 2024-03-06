from typing import Optional
from sqlmodel import Field, SQLModel, create_engine


class Vaga(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: Optional[int]= None
    register_date:Optional[str]=None
    company:Optional[str]=None
    title: Optional[str] = None
    location: Optional[str] = None
    time_opened: Optional[str] = None
    link: Optional[str] = None
    applications: Optional[str] = None
    experience_level: Optional[str] = None
    job_type: Optional[str] = None
    job_type: Optional[str] = None
    role: Optional[str] = None
    sectors: Optional[str] = None
    description: Optional[str] = None


sqlite_file_name = "database6.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, echo=True, connect_args=connect_args)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
