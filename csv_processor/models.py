from sqlalchemy.orm import declarative_base
from sqlalchemy import Integer, Column, Date, Float, String


# Define the ORM base
Base = declarative_base()


class Position(Base):
    __tablename__ = 'positions'
    id = Column(Integer, primary_key=True)
    # timestamp = Column(Integer())
    timestamp = Column(Date())
    symbol = Column(String(10))
    open_price = Column(Float)
    highest = Column(Float)
    lowest = Column(Float)
    close_price = Column(Float)

    def __repr__(self):
        return f"<Position(symbol='{self.symbol}', timestamp='{self.timestamp}'>"
