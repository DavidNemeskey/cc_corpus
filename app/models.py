from sqlalchemy import Column, Integer, String
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
        arguments = ["api_wrapper.py",
                     str(self.id),
                     self.script,
                     "-i", self.input,
                     "-o", self.output,
                     ]
        arguments += self.further_params.split(" ")
        # TODO: this should not be hardwired:
        LOG_DIR = Path("/mnt/d/coding/test_corpus2/logs")
        logfile = LOG_DIR / f"step_{self.id}_{self.script.split('.')[0]}.log"
        with open(logfile, 'w') as log_f:
            subprocess.Popen(arguments, stdout=log_f, stderr=log_f)
