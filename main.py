from fastapi import FastAPI
import uvicorn
from database import Database

app = FastAPI()
db = Database()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Thaliwada API"} 

@app.get("/items")
def read_items():
    items = db.fetch_items()
    return {"items": items} 

@app.get("/orders")
def read_orders():
    orders = db.fetch_orders_by_date("2023-10-01")
    return {"orders": orders}

@app.post("/items")
def create_item(name: str, description: str, price: float):
    db.insert_item(name, description, price)
    return {"message": f"Item '{name}' created successfully."}  

@app.post("/orders")
def create_order(date: str, status: str, customer_name: str, customer_contact: str, items: list):
    db.insert_order(date, status, customer_name, customer_contact, items)
    return {"message": "Order created successfully."}

@app.on_event("startup")
def startup_event():
    db.create_tables()
    print("Database tables created on startup.")    

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)