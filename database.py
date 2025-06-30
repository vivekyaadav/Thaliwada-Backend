from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, JSON, select, Date
import os
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("DATABASE_URL")

class Database:
    def __init__(self, db_url):
        self.engine = create_engine(db_url, echo=True)
        self.metadata = MetaData()

        self.items = Table(
            'items', self.metadata,
            Column('item_id', Integer, primary_key=True),
            Column('item_name', String, nullable=False),
            Column('item_description', String, nullable=False),
            Column('item_price', Integer, nullable=False)
        )

        self.orders = Table(
            'orders', self.metadata,
            Column('order_id', Integer, primary_key=True),
            Column('order_date', Date, nullable=False),
            Column('order_status', String, nullable=False),
            Column('customer_name', String, unique=True, nullable=False),
            Column('customer_contact', String, unique=True, nullable=False),
            Column('items', JSON, nullable=False)
        )
        
    def create_tables(self):
        self.metadata.create_all(self.engine)
        print("Tables created successfully.")

    def drop_tables(self):
        self.metadata.drop_all(self.engine)
        print("Tables dropped successfully.")

    def insert_item(self, item_name, item_description, item_price):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                ins = self.items.insert().values(
                    item_name=item_name,
                    item_description=item_description,
                    item_price=item_price
                )
                connection.execute(ins)
                trans.commit()
                print(f"Inserted item: {item_name}")
            except Exception as e:
                trans.rollback()
                print(f"Error inserting item: {e}")

    def insert_order(self, order_date, order_status, customer_name, customer_contact, items):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                ins = self.orders.insert().values(
                    order_date=order_date,
                    order_status=order_status,
                    customer_name=customer_name,
                    customer_contact=customer_contact,
                    items=items
                )
                connection.execute(ins)
            except Exception as e:
                trans.rollback()
                print(f"Error inserting order: {e}")

    def fetch_items(self):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                sel = select(self.items)
                result = connection.execute(sel)
                trans.commit()
                return result.fetchall()
            except Exception as e:
                trans.rollback()
                print(f"Error fetching items: {e}")
                return []
            
    def fetch_orders_by_date(self):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                sel = select(self.orders).order_by(self.orders.c.order_date)
                result = connection.execute(sel)
                trans.commit()
                return result.fetchall()
            except Exception as e:
                trans.rollback()
                print(f"Error fetching orders: {e}")
                return []
            
    def fetch_orders_by_status(self):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                sel = select(self.orders).order_by(self.orders.c.order_status)
                result = connection.execute(sel)
                trans.commit()
                return result.fetchall()
            except Exception as e:
                trans.rollback()
                print(f"Error fetching orders: {e}")
                return []

    def fetch_orders_by_customer(self):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                sel = select(self.orders).order_by(self.orders.c.customer_name)
                result = connection.execute(sel)
                trans.commit()
                return result.fetchall()
            except Exception as e:
                trans.rollback()
                print(f"Error fetching orders: {e}")
                return []

if __name__ == "__main__":
    create_tables()
    # drop_tables()
    print("Database setup complete.")