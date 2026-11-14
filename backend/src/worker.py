from .config.database import Base,engine,SessionLocal
from .services.execution import ensure_schema,queue
from .services.executor import Executor

def main():
    Base.metadata.create_all(engine)
    ensure_schema(engine)
    Executor(queue,SessionLocal).run_forever()

if __name__=="__main__":main()
