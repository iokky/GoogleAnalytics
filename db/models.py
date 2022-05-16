from sqlalchemy import Column, Integer, String, DECIMAL, UniqueConstraint
from .db import Base


class GaUsers(Base):
    __tablename__ = 'ga_users_dev'
    __table_args__ = (UniqueConstraint('index', name='user_index'),)

    # dimensions
    id = Column(Integer, autoincrement=True, unique=True, primary_key=True)
    date = Column(String(40))
    host_name = Column(String)
    source_medium = Column(String)
    campaign = Column(String(256))
    keyword = Column(String(256))
    device = Column(String(120))
    os = Column(String)

    # metrics
    users = Column(Integer)
    sessions = Column(Integer)

    # hash
    index = Column(String(256))

    def __init__(self,
                 date,
                 host_name,
                 source_medium,
                 campaign,
                 keyword,
                 device,
                 os,
                 users,
                 sessions,
                 index,
                 ):

        self.date = date
        self.host_name = host_name
        self.source_medium = source_medium
        self.campaign = campaign
        self.keyword = keyword
        self.device = device
        self.os = os
        self.users = users
        self.sessions = sessions
        self.index = index


class GaTransactions(Base):
    __tablename__ = 'ga_transactions_dev'
    __table_args__ = (UniqueConstraint('index', name='transaction_index'),)

    # dimensions
    id = Column(Integer, autoincrement=True, unique=True, primary_key=True)
    date = Column(String(40))
    host_name = Column(String)
    source_medium = Column(String)
    campaign = Column(String(256))
    keyword = Column(String(256))
    device = Column(String(120))
    os = Column(String)
    transaction_id = Column(Integer)

    # metrics
    transaction = Column(Integer)
    value = Column(DECIMAL)

    # hash
    index = Column(String(256))

    def __init__(self,
                 date,
                 host_name,
                 source_medium,
                 campaign,
                 keyword,
                 device,
                 os,
                 transaction_id,
                 transaction,
                 value,
                 index
                 ):

        self.date = date
        self.host_name = host_name
        self.source_medium = source_medium
        self.campaign = campaign
        self.keyword = keyword
        self.device = device
        self.os = os
        self.transaction_id = transaction_id
        self.transaction = transaction
        self.value = value
        self.index = index

    def __str__(self):
        return f'{self.transaction_id}'
