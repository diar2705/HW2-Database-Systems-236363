from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Customer import Customer, BadCustomer
from Business.Order import Order, BadOrder
from Business.Dish import Dish, BadDish
from Business.OrderDish import OrderDish


# ---------------------------------- CRUD API: ----------------------------------
# Basic database functions


def create_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "CREATE TABLE Customers(\
                    cust_id INTEGER PRIMARY KEY CHECK(cust_id > 0),\
                    full_name TEXT NOT NULL,\
                    age INTEGER NOT NULL CHECK(age BETWEEN 18 AND 120),\
                    phone TEXT NOT NULL CHECK(LENGTH(phone) = 10)\
                   )"
        )

        conn.execute(
            "CREATE TABLE Orders(\
                    order_id INTEGER PRIMARY KEY CHECK(order_id > 0),\
                    cust_id INTEGER,\
                    date TIMESTAMP NOT NULL,\
                    delivery_fee FLOAT NOT NULL CHECK(delivery_fee >= 0),\
                    delivery_address TEXT NOT NULL,\
                    FOREIGN KEY(cust_id) REFERENCES Customers(cust_id)\
                   )"
        )

        conn.execute(
            "CREATE TABLE Dishes(\
                    dish_id INTEGER PRIMARY KEY CHECK(dish_id > 0),\
                    name TEXT NOT NULL CHECK(LENGTH(name) >= 4),\
                    price FLOAT NOT NULL CHECK(price > 0),\
                    is_active BOOLEAN NOT NULL\
                   )"
        )

        conn.execute(
            "CREATE TABLE OrderDishes(\
                    order_id INTEGER,\
                    dish_id INTEGER,\
                    amount INTEGER NOT NULL CHECK(amount > 0),\
                    FOREIGN KEY(order_id) REFERENCES Orders(order_id) ON DELETE CASCADE,\
                    FOREIGN KEY(dish_id) REFERENCES Dishes(dish_id) ON DELETE CASCADE,\
                    PRIMARY KEY(order_id, dish_id)\
                   )"
        )

        conn.execute(
            "CREATE TABLE Ratings(\
                    cust_id INTEGER,\
                    dish_id INTEGER,\
                    rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),\
                    FOREIGN KEY(cust_id) REFERENCES Customers(cust_id) ON DELETE CASCADE,\
                    FOREIGN KEY(dish_id) REFERENCES Dishes(dish_id) ON DELETE CASCADE,\
                    PRIMARY KEY(cust_id, dish_id)\
                   )"
        )

        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def clear_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Ratings")
        conn.execute("DELETE FROM OrderDishes")
        conn.execute("DELETE FROM Orders")
        conn.execute("DELETE FROM Dishes")
        conn.execute("DELETE FROM Customers")
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def drop_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Ratings")
        conn.execute("DROP TABLE IF EXISTS OrderDishes")
        conn.execute("DROP TABLE IF EXISTS Orders")
        conn.execute("DROP TABLE IF EXISTS Dishes")
        conn.execute("DROP TABLE IF EXISTS Customers")
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


# CRUD API

def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if (
            customer.get_cust_id() is None
            or customer.get_full_name() is None
            or customer.get_phone() is None
            or customer.get_age() is None
            or customer.get_cust_id() <= 0
            or customer.get_age() < 18
            or customer.get_age() > 120
            or len(customer.get_phone()) != 10
        ):
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Customers(customer_id, full_name, phone, age) VALUES({id}, {name}, {phone}, {age})"
        ).format(
            id=sql.Literal(customer.get_cust_id()),
            name=sql.Literal(customer.get_full_name()),
            phone=sql.Literal(customer.get_phone()),
            age=sql.Literal(customer.get_age()),
        )

        _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_customer(customer_id: int) -> Customer:
    conn = None
    try:
        if customer_id <= 0:
            return BadCustomer()

        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(customer_id)
        )

        _, result = conn.execute(query)

        # If no customer found
        if result.isEmpty():
            return BadCustomer()

        # Get first (and only) row of results
        row = result[0]
        customer = Customer(
            cust_id=row["customer_id"],
            full_name=row["full_name"],
            phone=row["phone"],
            age=row["age"],
        )

        return customer
    except Exception as e:
        if conn:
            conn.rollback()
        return BadCustomer()
    finally:
        if conn:
            conn.close()


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    try:
        if customer_id <= 0:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()
        # First check if customer exists
        query = sql.SQL("SELECT * FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(customer_id)
        )

        _, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Delete the customer
        query = sql.SQL("DELETE FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(customer_id)
        )

        _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def add_order(order: Order) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if (
            order.get_order_id() is None
            or order.get_datetime() is None
            or order.get_delivery_fee() is None
            or order.get_delivery_address() is None
            or order.get_order_id() <= 0
            or order.get_delivery_fee() < 0
        ):
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Orders(order_id, date, delivery_fee, delivery_address) VALUES({id}, {date}, {fee}, {address})"
        ).format(
            id=sql.Literal(order.get_order_id()),
            date=sql.Literal(order.get_datetime()),
            fee=sql.Literal(order.get_delivery_fee()),
            address=sql.Literal(order.get_delivery_address()),
        )

        _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_order(order_id: int) -> Order:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )

        _, result = conn.execute(query)

        # If no order found
        if result.isEmpty():
            return BadOrder()

        # Get first (and only) row of results
        row = result[0]
        order = Order(
            order_id=row["order_id"],
            date=row["date"],
            delivery_fee=row["delivery_fee"],
            delivery_address=row["delivery_address"],
        )

        return order
    except Exception as e:
        if conn:
            conn.rollback()
        return BadOrder()
    finally:
        if conn:
            conn.close()


def delete_order(order_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        # First check if order exists
        query = sql.SQL("SELECT * FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )

        _, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Delete the order
        query = sql.SQL("DELETE FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )

        _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def add_dish(dish: Dish) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if (
            dish.get_dish_id() is None
            or dish.get_name() is None
            or dish.get_price() is None
            or dish.get_is_active() is None
            or dish.get_dish_id() <= 0
            or dish.get_price() <= 0
        ):
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Dishes(dish_id, name, price, is_active) VALUES({id}, {name}, {price}, {active})"
        ).format(
            id=sql.Literal(dish.get_dish_id()),
            name=sql.Literal(dish.get_name()),
            price=sql.Literal(dish.get_price()),
            active=sql.Literal(dish.get_is_active()),
        )

        _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_dish(dish_id: int) -> Dish:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Dishes WHERE dish_id = {id}").format(
            id=sql.Literal(dish_id)
        )

        _, result = conn.execute(query)

        # If no dish found
        if result.isEmpty():
            return BadDish()

        # Get first (and only) row of results
        row = result[0]
        dish = Dish(
            dish_id=row["dish_id"],
            name=row["name"],
            price=row["price"],
            is_active=row["is_active"],
        )

        return dish
    except Exception as e:
        if conn:
            conn.rollback()
        return BadDish()
    finally:
        if conn:
            conn.close()


def update_dish_price(dish_id: int, price: float) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if dish_id <= 0 or price <= 0:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()
        # First check if dish exists
        query = sql.SQL("SELECT * FROM Dishes WHERE dish_id = {id}").format(
            id=sql.Literal(dish_id)
        )

        _, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Update the dish price
        query = sql.SQL(
            "UPDATE Dishes SET price = {price} WHERE dish_id = {id}"
        ).format(price=sql.Literal(price), id=sql.Literal(dish_id))

        _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def update_dish_active_status(dish_id: int, is_active: bool) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if dish_id <= 0:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()
        # First check if dish exists
        query = sql.SQL("SELECT * FROM Dishes WHERE dish_id = {id}").format(
            id=sql.Literal(dish_id)
        )

        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Update the dish active status
        query = sql.SQL(
            "UPDATE Dishes SET is_active = {active} WHERE dish_id = {id}"
        ).format(active=sql.Literal(is_active), id=sql.Literal(dish_id))

        rows_affected = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def customer_placed_order(customer_id: int, order_id: int) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if customer_id <= 0 or order_id <= 0:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()

        # Check if the customer exists
        query = sql.SQL("SELECT * FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(customer_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Check if the order exists
        query = sql.SQL("SELECT * FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Check if order already has a customer
        query = sql.SQL(
            "SELECT customer_id FROM Orders WHERE order_id = {id} AND customer_id IS NOT NULL"
        ).format(id=sql.Literal(order_id))
        rows_affected, result = conn.execute(query)

        if not result.isEmpty():
            return ReturnValue.ALREADY_EXISTS

        # Update the order with the customer_id
        query = sql.SQL(
            "UPDATE Orders SET customer_id = {cust_id} WHERE order_id = {order_id}"
        ).format(cust_id=sql.Literal(customer_id), order_id=sql.Literal(order_id))

        rows_affected = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_customer_that_placed_order(order_id: int) -> Customer:
    conn = None
    try:
        # Check if inputs are valid
        if order_id <= 0:
            return BadCustomer()

        conn = Connector.DBConnector()

        # Check if the order exists and get the associated customer
        query = sql.SQL(
            "SELECT c.* FROM Customers c JOIN Orders o ON c.customer_id = o.customer_id WHERE o.order_id = {id}"
        ).format(id=sql.Literal(order_id))

        rows_affected, result = conn.execute(query)

        # If no customer found for this order
        if result.isEmpty():
            return BadCustomer()

        # Get first (and only) row of results
        row = result[0]
        customer = Customer(
            cust_id=row["customer_id"],
            full_name=row["full_name"],
            phone=row["phone"],
            age=row["age"],
        )

        return customer
    except Exception as e:
        if conn:
            conn.rollback()
        return BadCustomer()
    finally:
        if conn:
            conn.close()


def order_contains_dish(order_id: int, dish_id: int, amount: int) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if order_id <= 0 or dish_id <= 0 or amount <= 0:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()

        # Check if the order exists
        query = sql.SQL("SELECT * FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Check if the dish exists
        query = sql.SQL("SELECT * FROM Dishes WHERE dish_id = {id}").format(
            id=sql.Literal(dish_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Check if dish is active
        row = result[0]
        if not row["is_active"]:
            return ReturnValue.BAD_PARAMS

        # Check if the order already contains this dish
        query = sql.SQL(
            "SELECT * FROM OrderDishes WHERE order_id = {order_id} AND dish_id = {dish_id}"
        ).format(order_id=sql.Literal(order_id), dish_id=sql.Literal(dish_id))
        rows_affected, result = conn.execute(query)

        if not result.isEmpty():
            # Order already contains this dish, update the amount
            query = sql.SQL(
                "UPDATE OrderDishes SET amount = {amount} WHERE order_id = {order_id} AND dish_id = {dish_id}"
            ).format(
                amount=sql.Literal(amount),
                order_id=sql.Literal(order_id),
                dish_id=sql.Literal(dish_id),
            )
        else:
            # Order does not contain this dish, insert a new record
            query = sql.SQL(
                "INSERT INTO OrderDishes(order_id, dish_id, amount) VALUES({order_id}, {dish_id}, {amount})"
            ).format(
                order_id=sql.Literal(order_id),
                dish_id=sql.Literal(dish_id),
                amount=sql.Literal(amount),
            )

        rows_affected = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def order_does_not_contain_dish(order_id: int, dish_id: int) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if order_id <= 0 or dish_id <= 0:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()

        # Check if the relationship between order and dish exists
        query = sql.SQL(
            "SELECT * FROM OrderDishes WHERE order_id = {order_id} AND dish_id = {dish_id}"
        ).format(order_id=sql.Literal(order_id), dish_id=sql.Literal(dish_id))
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Delete the relationship
        query = sql.SQL(
            "DELETE FROM OrderDishes WHERE order_id = {order_id} AND dish_id = {dish_id}"
        ).format(order_id=sql.Literal(order_id), dish_id=sql.Literal(dish_id))

        rows_affected = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def customer_rated_dish(cust_id: int, dish_id: int, rating: int) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if cust_id <= 0 or dish_id <= 0 or rating < 1 or rating > 5:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()

        # Check if the customer exists
        query = sql.SQL("SELECT * FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(cust_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Check if the dish exists
        query = sql.SQL("SELECT * FROM Dishes WHERE dish_id = {id}").format(
            id=sql.Literal(dish_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Check if the customer has already rated this dish
        query = sql.SQL(
            "SELECT * FROM Ratings WHERE customer_id = {cust_id} AND dish_id = {dish_id}"
        ).format(cust_id=sql.Literal(cust_id), dish_id=sql.Literal(dish_id))
        rows_affected, result = conn.execute(query)

        if not result.isEmpty():
            # Customer has already rated this dish, update the rating
            query = sql.SQL(
                "UPDATE Ratings SET rating = {rating} WHERE customer_id = {cust_id} AND dish_id = {dish_id}"
            ).format(
                rating=sql.Literal(rating),
                cust_id=sql.Literal(cust_id),
                dish_id=sql.Literal(dish_id),
            )
        else:
            # Customer has not rated this dish, insert a new rating
            query = sql.SQL(
                "INSERT INTO Ratings(customer_id, dish_id, rating) VALUES({cust_id}, {dish_id}, {rating})"
            ).format(
                cust_id=sql.Literal(cust_id),
                dish_id=sql.Literal(dish_id),
                rating=sql.Literal(rating),
            )

        rows_affected = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def customer_deleted_rating_on_dish(cust_id: int, dish_id: int) -> ReturnValue:
    conn = None
    try:
        # Check if inputs are valid
        if cust_id <= 0 or dish_id <= 0:
            return ReturnValue.BAD_PARAMS

        conn = Connector.DBConnector()

        # Check if the rating exists
        query = sql.SQL(
            "SELECT * FROM Ratings WHERE customer_id = {cust_id} AND dish_id = {dish_id}"
        ).format(cust_id=sql.Literal(cust_id), dish_id=sql.Literal(dish_id))
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return ReturnValue.NOT_EXISTS

        # Delete the rating
        query = sql.SQL(
            "DELETE FROM Ratings WHERE customer_id = {cust_id} AND dish_id = {dish_id}"
        ).format(cust_id=sql.Literal(cust_id), dish_id=sql.Literal(dish_id))

        rows_affected = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except Exception as e:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_all_customer_ratings(cust_id: int) -> List[Tuple[int, int]]:
    conn = None
    try:
        # Check if inputs are valid
        if cust_id <= 0:
            return []

        conn = Connector.DBConnector()

        # Check if the customer exists
        query = sql.SQL("SELECT * FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(cust_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return []

        # Get all ratings by the customer
        query = sql.SQL(
            """
            SELECT dish_id, rating
            FROM Ratings
            WHERE customer_id = {id}
            ORDER BY dish_id
        """
        ).format(id=sql.Literal(cust_id))

        rows_affected, result = conn.execute(query)

        # If no ratings found
        if result.isEmpty():
            return []

        # Convert result to a list of tuples (dish_id, rating)
        ratings = []
        for row in result:
            ratings.append((row["dish_id"], row["rating"]))

        return ratings
    except Exception as e:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_all_order_items(order_id: int) -> List[OrderDish]:
    conn = None
    try:
        # Check if inputs are valid
        if order_id <= 0:
            return []

        conn = Connector.DBConnector()

        # Check if the order exists
        query = sql.SQL("SELECT * FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return []

        # Get all dishes in the order along with their prices
        query = sql.SQL(
            """
            SELECT od.dish_id, od.amount, d.price
            FROM OrderDishes od
            JOIN Dishes d ON od.dish_id = d.dish_id
            WHERE od.order_id = {id}
            ORDER BY od.dish_id
        """
        ).format(id=sql.Literal(order_id))

        rows_affected, result = conn.execute(query)

        # If no dishes in the order
        if result.isEmpty():
            return []

        # Convert result to a list of OrderDish objects
        order_items = []
        for row in result:
            order_dish = OrderDish(
                dish_id=row["dish_id"], amount=row["amount"], price=row["price"]
            )
            order_items.append(order_dish)

        return order_items
    except Exception as e:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


# ---------------------------------- BASIC API: ----------------------------------

# Basic API


def get_order_total_price(order_id: int) -> float:
    conn = None
    try:
        # Check if inputs are valid
        if order_id <= 0:
            return 0

        conn = Connector.DBConnector()

        # Check if the order exists
        query = sql.SQL("SELECT * FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return 0

        # Get the delivery fee for the order
        delivery_fee = result[0]["delivery_fee"]

        # Calculate the total price of all dishes in the order
        query = sql.SQL(
            """
            SELECT SUM(od.amount * d.price) as dishes_price
            FROM OrderDishes od
            JOIN Dishes d ON od.dish_id = d.dish_id
            WHERE od.order_id = {id}
        """
        ).format(id=sql.Literal(order_id))

        rows_affected, result = conn.execute(query)

        # If no dishes in the order, return just the delivery fee
        if result.isEmpty() or result[0]["dishes_price"] is None:
            return delivery_fee

        # Return the total price (sum of dish prices * their amounts + delivery fee)
        return result[0]["dishes_price"] + delivery_fee
    except Exception as e:
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            conn.close()


def get_customers_spent_max_avg_amount_money() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()

        # Find customers with the highest average order total price
        query = sql.SQL(
            """
            WITH OrderTotals AS (
                SELECT 
                    o.customer_id,
                    o.order_id,
                    o.delivery_fee + COALESCE(SUM(od.amount * d.price), 0) AS total_price
                FROM 
                    Orders o
                LEFT JOIN 
                    OrderDishes od ON o.order_id = od.order_id
                LEFT JOIN 
                    Dishes d ON od.dish_id = d.dish_id
                WHERE 
                    o.customer_id IS NOT NULL
                GROUP BY 
                    o.order_id, o.customer_id, o.delivery_fee
            ),
            CustomerAvgs AS (
                SELECT 
                    customer_id,
                    AVG(total_price) AS avg_total
                FROM 
                    OrderTotals
                GROUP BY 
                    customer_id
            ),
            MaxAvg AS (
                SELECT 
                    MAX(avg_total) AS max_avg_total
                FROM 
                    CustomerAvgs
            )
            SELECT 
                c.customer_id
            FROM 
                CustomerAvgs c, MaxAvg m
            WHERE 
                c.avg_total = m.max_avg_total
            ORDER BY 
                c.customer_id
        """
        )

        rows_affected, result = conn.execute(query)

        # If no customers found
        if result.isEmpty():
            return []

        # Extract customer IDs
        customer_ids = []
        for row in result:
            customer_ids.append(row["customer_id"])

        return customer_ids
    except Exception as e:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_most_ordered_dish_in_period(start: datetime, end: datetime) -> Dish:
    conn = None
    try:
        # Check if inputs are valid
        if start is None or end is None or start > end:
            return BadDish()

        conn = Connector.DBConnector()

        # Find the dish with the highest total amount ordered in the given period
        query = sql.SQL(
            """
            WITH DishAmounts AS (
                SELECT 
                    od.dish_id,
                    SUM(od.amount) AS total_amount
                FROM 
                    OrderDishes od
                JOIN 
                    Orders o ON od.order_id = o.order_id
                WHERE 
                    o.date >= {start} AND o.date <= {end}
                GROUP BY 
                    od.dish_id
            ),
            MaxAmount AS (
                SELECT 
                    MAX(total_amount) AS max_total_amount
                FROM 
                    DishAmounts
            )
            SELECT 
                d.*
            FROM 
                Dishes d
            JOIN 
                DishAmounts da ON d.dish_id = da.dish_id
            JOIN 
                MaxAmount ma ON da.total_amount = ma.max_total_amount
            ORDER BY 
                d.dish_id
            LIMIT 1
        """
        ).format(start=sql.Literal(start), end=sql.Literal(end))

        rows_affected, result = conn.execute(query)

        # If no dish found
        if result.isEmpty():
            return BadDish()

        # Get the first (and possibly only) row of results
        row = result[0]
        dish = Dish(
            dish_id=row["dish_id"],
            name=row["name"],
            price=row["price"],
            is_active=row["is_active"],
        )

        return dish
    except Exception as e:
        if conn:
            conn.rollback()
        return BadDish()
    finally:
        if conn:
            conn.close()


def did_customer_order_top_rated_dishes(cust_id: int) -> bool:
    conn = None
    try:
        # Check if inputs are valid
        if cust_id <= 0:
            return False

        conn = Connector.DBConnector()

        # Check if the customer exists
        query = sql.SQL("SELECT * FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(cust_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return False

        # Check if the customer has any orders
        query = sql.SQL("SELECT * FROM Orders WHERE customer_id = {id}").format(
            id=sql.Literal(cust_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return False

        # Find if the customer ordered at least one of the top-rated dishes
        # Top-rated dishes are those with the highest average rating
        query = sql.SQL(
            """
            WITH TopRatedDishes AS (
                WITH DishAvgRatings AS (
                    SELECT 
                        dish_id,
                        AVG(rating) AS avg_rating
                    FROM 
                        Ratings
                    GROUP BY 
                        dish_id
                ),
                MaxRating AS (
                    SELECT 
                        MAX(avg_rating) AS max_avg_rating
                    FROM 
                        DishAvgRatings
                )
                SELECT 
                    d.dish_id
                FROM 
                    DishAvgRatings d, MaxRating m
                WHERE 
                    d.avg_rating = m.max_avg_rating
            )
            SELECT 
                COUNT(*) AS ordered_top_rated
            FROM 
                Orders o
            JOIN 
                OrderDishes od ON o.order_id = od.order_id
            JOIN 
                TopRatedDishes t ON od.dish_id = t.dish_id
            WHERE 
                o.customer_id = {id}
        """
        ).format(id=sql.Literal(cust_id))

        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return False

        # If the customer has ordered at least one top-rated dish
        return result[0]["ordered_top_rated"] > 0
    except Exception as e:
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


# ---------------------------------- ADVANCED API: ----------------------------------

# Advanced API


def get_customers_rated_but_not_ordered() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()

        # Find customers who have rated dishes but never ordered any of those dishes
        query = sql.SQL(
            """
            SELECT DISTINCT r.customer_id
            FROM Ratings r
            WHERE NOT EXISTS (
                SELECT 1
                FROM Orders o
                JOIN OrderDishes od ON o.order_id = od.order_id
                WHERE o.customer_id = r.customer_id AND od.dish_id = r.dish_id
            )
            ORDER BY r.customer_id
        """
        )

        rows_affected, result = conn.execute(query)

        # If no customers found
        if result.isEmpty():
            return []

        # Extract customer IDs
        customer_ids = []
        for row in result:
            customer_ids.append(row["customer_id"])

        return customer_ids
    except Exception as e:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_non_worth_price_increase() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()

        # Find dishes that had a price increase but lower order amounts afterward
        query = sql.SQL(
            """
            WITH PriceChanges AS (
                SELECT
                    o.order_id,
                    o.date,
                    od.dish_id,
                    od.amount,
                    d.price,
                    LAG(d.price) OVER (PARTITION BY od.dish_id ORDER BY o.date) AS prev_price
                FROM
                    OrderDishes od
                JOIN
                    Orders o ON od.order_id = o.order_id
                JOIN
                    Dishes d ON od.dish_id = d.dish_id
            ),
            DishPeriods AS (
                SELECT
                    dish_id,
                    date,
                    amount,
                    price,
                    prev_price,
                    CASE WHEN price > prev_price THEN 1 ELSE 0 END AS price_increased
                FROM
                    PriceChanges
                WHERE
                    prev_price IS NOT NULL
            ),
            DishStats AS (
                SELECT
                    dish_id,
                    SUM(CASE WHEN price_increased = 1 THEN 1 ELSE 0 END) AS has_price_increase,
                    SUM(CASE WHEN price_increased = 1 THEN amount ELSE 0 END) AS amount_after_increase,
                    SUM(CASE WHEN price_increased = 0 THEN amount ELSE 0 END) AS amount_before_increase
                FROM
                    DishPeriods
                GROUP BY
                    dish_id
            )
            SELECT
                dish_id
            FROM
                DishStats
            WHERE
                has_price_increase > 0
                AND amount_after_increase < amount_before_increase
            ORDER BY
                dish_id
        """
        )

        rows_affected, result = conn.execute(query)

        # If no dishes found
        if result.isEmpty():
            return []

        # Extract dish IDs
        dish_ids = []
        for row in result:
            dish_ids.append(row["dish_id"])

        return dish_ids
    except Exception as e:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_cumulative_profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    try:
        # Check if inputs are valid
        if year <= 0:
            return []

        conn = Connector.DBConnector()

        # Calculate cumulative profit for each month in the given year
        query = sql.SQL(
            """
            WITH MonthlyOrders AS (
                SELECT
                    EXTRACT(MONTH FROM o.date) AS month,
                    o.order_id,
                    o.delivery_fee,
                    SUM(od.amount * d.price) AS dishes_total
                FROM
                    Orders o
                JOIN
                    OrderDishes od ON o.order_id = od.order_id
                JOIN
                    Dishes d ON od.dish_id = d.dish_id
                WHERE
                    EXTRACT(YEAR FROM o.date) = {year}
                GROUP BY
                    month, o.order_id, o.delivery_fee
            ),
            MonthlyProfit AS (
                SELECT
                    month,
                    SUM(delivery_fee + dishes_total) AS monthly_profit
                FROM
                    MonthlyOrders
                GROUP BY
                    month
                ORDER BY
                    month
            ),
            CumulativeProfit AS (
                SELECT
                    month,
                    SUM(monthly_profit) OVER (ORDER BY month) AS cumulative_profit
                FROM
                    MonthlyProfit
            )
            SELECT
                month::int,
                cumulative_profit
            FROM
                CumulativeProfit
            ORDER BY
                month
        """
        ).format(year=sql.Literal(year))

        rows_affected, result = conn.execute(query)

        # If no data found
        if result.isEmpty():
            return []

        # Convert result to a list of tuples (month, cumulative_profit)
        monthly_profits = []
        for row in result:
            monthly_profits.append((row["month"], row["cumulative_profit"]))

        return monthly_profits
    except Exception as e:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_potential_dish_recommendations(cust_id: int) -> List[int]:
    conn = None
    try:
        # Check if inputs are valid
        if cust_id <= 0:
            return []

        conn = Connector.DBConnector()

        # Check if the customer exists
        query = sql.SQL("SELECT * FROM Customers WHERE customer_id = {id}").format(
            id=sql.Literal(cust_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return []

        # Find dishes that similar customers ordered but this customer hasn't
        # Two customers are similar if they both rated the same dish with the same rating at least once
        query = sql.SQL(
            """
            WITH SimilarCustomers AS (
                -- Customers who gave the same rating to the same dish as our customer
                SELECT DISTINCT r2.customer_id
                FROM Ratings r1
                JOIN Ratings r2 ON r1.dish_id = r2.dish_id AND r1.rating = r2.rating
                WHERE r1.customer_id = {cust_id} AND r2.customer_id != {cust_id}
            ),
            OrderedByOthers AS (
                -- Dishes ordered by similar customers
                SELECT DISTINCT od.dish_id
                FROM OrderDishes od
                JOIN Orders o ON od.order_id = o.order_id
                WHERE o.customer_id IN (SELECT customer_id FROM SimilarCustomers)
            ),
            OrderedByCustomer AS (
                -- Dishes already ordered by our customer
                SELECT DISTINCT od.dish_id
                FROM OrderDishes od
                JOIN Orders o ON od.order_id = o.order_id
                WHERE o.customer_id = {cust_id}
            ),
            RecommendedDishes AS (
                -- Dishes ordered by similar customers but not by our customer
                SELECT dish_id
                FROM OrderedByOthers
                EXCEPT
                SELECT dish_id
                FROM OrderedByCustomer
            )
            SELECT dish_id
            FROM RecommendedDishes
            ORDER BY dish_id
        """
        ).format(cust_id=sql.Literal(cust_id))

        rows_affected, result = conn.execute(query)

        # If no recommendations found
        if result.isEmpty():
            return []

        # Extract recommended dish IDs
        recommended_dishes = []
        for row in result:
            recommended_dishes.append(row["dish_id"])

        return recommended_dishes
    except Exception as e:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()
