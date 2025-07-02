from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, JSON, select, Date
import os
from dotenv import load_dotenv



class Database:
    def __init__(self):
        load_dotenv()
        # Load the database URL from environment variables
        db_url = os.getenv("DB_URL")
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
            
    def fetch_orders_by_date(self, date):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                sel = select(self.orders).where(self.orders.c.order_date == date)
                result = connection.execute(sel)
                trans.commit()
                return result.fetchall()
            except Exception as e:
                trans.rollback()
                print(f"Error fetching orders: {e}")
                return []
            
    def fetch_orders_by_status(self, status):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                sel = select(self.orders).where(self.orders.c.order_status == status)
                result = connection.execute(sel)
                trans.commit()
                return result.fetchall()
            except Exception as e:
                trans.rollback()
                print(f"Error fetching orders: {e}")
                return []

    def fetch_orders_by_customer(self, customer_name):
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                sel = select(self.orders).where(self.orders.c.customer_name == customer_name)
                result = connection.execute(sel)
                trans.commit()
                return result.fetchall()
            except Exception as e:
                trans.rollback()
                print(f"Error fetching orders: {e}")
                return []

if __name__ == "__main__":
    db = Database()
    # db.insert_item("Indian Thali",
    #                 "Rich and creamy black lentils slow-cooked with butter, cream, and aromatic spices, served with basmati rice, naan, pickle, and raita",
    #                 299)
    # drop_tables()
    print(db.fetch_items())
    print("Database setup complete.")