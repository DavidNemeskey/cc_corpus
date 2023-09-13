from sqlalchemy import Column, ForeignKey, Integer, String
from pathlib import Path
import subprocess

from .database import Base

class Step(Base):
    __tablename__ = "steps"

    id = Column(Integer, primary_key=True, index=True)
    script = Column(String)
    script_version = Column(String)
    comment = Column(String)
    status = Column(String)
    input = Column(String)
    output = Column(String)
    further_params = Column(String)

    def run_script(self):
        arguments = [self.script,
                     "-i", self.input,
                     "-o", self.output,
                     "--sid", str(self.id),
                     ]
        arguments += self.further_params.split(" ")
        LOG_DIR = Path("/mnt/d/coding/test_corpus2/logs")
        logfile = LOG_DIR / f"step_{self.id}_{self.script.split('.')[0]}.log"
        with open(logfile, 'w') as log_f:
            subprocess.Popen(arguments, stdout=log_f, stderr=log_f)



