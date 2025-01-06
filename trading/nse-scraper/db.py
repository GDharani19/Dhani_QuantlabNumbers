# # db.py

# from sqlalchemy import create_engine
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker

# from config import settings

# # Create the database engine
# engine = create_engine(settings.DATABASE_URL,
#                        pool_recycle=300, pool_pre_ping=True)

# # Create a sessionmaker for the database engine
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # Create a base class for declarative class definitions
# Base = declarative_base()


# def get_db():
#     """
#     Function to get a database session.

#     Returns:
#         sqlalchemy.orm.Session: A database session.

#     Yields:
#         sqlalchemy.orm.Session: A database session.
#     """
#     # Create a new session
#     db = SessionLocal()
#     try:
#         # Yield the session to the caller
#         yield db
#     finally:
#         # Close the session after it's used
#         db.close()


from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config import settings

# Create the database engine
# engine = create_engine(settings.DATABASE_URL,
#                        pool_recycle=300, pool_pre_ping=True)

# DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/testdb"
engine = create_engine(settings.DATABASE_URL,
                        pool_recycle=300, pool_pre_ping=True)

# Create a sessionmaker for the database engine
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a base class for declarative class definitions
from sqlalchemy.orm import declarative_base as new_declarative_base
Base = new_declarative_base()

def get_db():
    """
    Function to get a database session.

    Returns:
        sqlalchemy.orm.Session: A database session.

    Yields:
        sqlalchemy.orm.Session: A database session.
    """
    # Create a new session
    db = SessionLocal()
    try:
        # Yield the session to the caller
        yield db
    finally:
        # Close the session after it's used
        db.close()
