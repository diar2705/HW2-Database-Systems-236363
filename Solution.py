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
from Utility.DBConnector import ResultSet


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
                    date TIMESTAMP NOT NULL,\
                    delivery_fee DECIMAL NOT NULL CHECK(delivery_fee >= 0),\
                    delivery_address TEXT NOT NULL CHECK(LENGTH(delivery_address) >= 5)\
                   )"
        )

        conn.execute(
            "CREATE TABLE Dishs(\
                    dish_id INTEGER PRIMARY KEY CHECK(dish_id > 0),\
                    name TEXT NOT NULL CHECK(LENGTH(name) >= 4),\
                    price DECIMAL NOT NULL CHECK(price > 0),\
                    is_active BOOLEAN NOT NULL\
                   )"
        )

        conn.execute(
            "CREATE TABLE CustomerOrders(\
                    cust_id INTEGER,\
                    order_id INTEGER,\
                    FOREIGN KEY(cust_id) REFERENCES Customers(cust_id) ON DELETE CASCADE,\
                    FOREIGN KEY(order_id) REFERENCES Orders(order_id) ON DELETE CASCADE,\
                    PRIMARY KEY(order_id)\
                   )"
        )

        conn.execute(
            "CREATE TABLE DishOrders(\
                    order_id INTEGER,\
                    dish_id INTEGER,\
                    amount INTEGER NOT NULL CHECK(amount >= 0),\
                    price DECIMAL NOT NULL,\
                    FOREIGN KEY(order_id) REFERENCES Orders(order_id) ON DELETE CASCADE,\
                    FOREIGN KEY(dish_id) REFERENCES Dishs(dish_id) ON DELETE CASCADE,\
                    PRIMARY KEY(order_id, dish_id)\
                   )"
        )

        conn.execute(
            "CREATE TABLE Ratings(\
                    cust_id INTEGER,\
                    dish_id INTEGER,\
                    rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),\
                    FOREIGN KEY(cust_id) REFERENCES Customers(cust_id) ON DELETE CASCADE,\
                    FOREIGN KEY(dish_id) REFERENCES Dishs(dish_id) ON DELETE CASCADE,\
                    PRIMARY KEY(cust_id, dish_id)\
                   )"
        )
        
        conn.execute(
            "CREATE VIEW totalPricePerOrder AS ( "
            "SELECT  O.order_id AS order_id, (COALESCE(SUM(D.amount * D.price), 0) + O.delivery_fee) AS totalPrice "
            "FROM DishOrders D RIGHT OUTER JOIN Orders O ON O.order_id = D.order_id GROUP BY O.order_id, O.delivery_fee)"
        )

        conn.execute(
            "CREATE VIEW SortRatingsDesc AS ( "
            "SELECT D.dish_id, COALESCE(AVG(DR.rating),3) AS avgRating "
            "FROM Ratings DR RIGHT OUTER JOIN Dish D ON D.dish_id = DR.dish_id "
            "GROUP BY D.dish_id "
            "ORDER BY avgRating DESC, D.dish_id ASC "
            "LIMIT 5)"
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
        conn.execute("DELETE FROM CustomerOrders")
        conn.execute("DELETE FROM Ratings")
        conn.execute("DELETE FROM DishOrders")
        conn.execute("DELETE FROM Customers")
        conn.execute("DELETE FROM Orders")
        conn.execute("DELETE FROM Dishs")
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
        conn.execute("DROP VIEW IF EXISTS totalPricePerOrder")
        conn.execute("DROP TABLE IF EXISTS Customers CASCADE")
        conn.execute("DROP TABLE IF EXISTS Orders CASCADE")
        conn.execute("DROP TABLE IF EXISTS Dishs CASCADE")
        conn.execute("DROP TABLE IF EXISTS DishOrders CASCADE")
        conn.execute("DROP TABLE IF EXISTS CustomerOrders CASCADE")
        conn.execute("DROP TABLE IF EXISTS Ratings CASCADE")
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


# CRUD API

#TODO: do we need to use rollback for each except?
def add_customer(customer: Customer) -> ReturnValue:
    conn = None

    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Customers(cust_id, full_name, phone, age) VALUES({id}, {name}, {phone}, {age})"
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
        query = sql.SQL("SELECT * FROM Customers WHERE cust_id = {id}").format(
            id=sql.Literal(customer_id)
        )

        num_rows, result = conn.execute(query)

        # If no customer found
        if result.isEmpty():
            return BadCustomer()

        # Get first (and only) row of results
        row = result[0]
        customer = Customer()
        customer.set_cust_id(row["cust_id"])
        customer.set_full_name(row["full_name"])
        customer.set_phone(row["phone"])
        #TODO: error in the customer file
        customer.set_address(row["age"])
        # customer = Customer(
        #     cust_id=row["cust_id"],
        #     full_name=row["full_name"],
        #     phone=row["phone"],
        #     age=row["age"],
        # )

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
        # Delete the customer
        query = sql.SQL("DELETE FROM Customers WHERE cust_id = {id}").format(
            id=sql.Literal(customer_id)
        )

        rows, _ = conn.execute(query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
        
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
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Orders(order_id, date, delivery_fee, delivery_address) VALUES({id}, {date_time}, {fee}, {address})"
        ).format(
            id=sql.Literal(order.get_order_id()),
            date_time=sql.Literal(order.get_datetime()),
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

        # Delete the order
        query = sql.SQL("DELETE FROM Orders WHERE order_id = {id}").format(
            id=sql.Literal(order_id)
        )

        rows, _ = conn.execute(query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
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

        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Dishs(dish_id, name, price, is_active) VALUES({id}, {name}, {price}, {active})"
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
        query = sql.SQL("SELECT * FROM Dishs WHERE dish_id = {id}").format(
            id=sql.Literal(dish_id)
        )

        _, result = conn.execute(query)

        # If no dish found
        if result.isEmpty():
            return BadDish()

        # Get first (and only) row of results
        row = result[0]
        dish = Dish()
        dish.set_dish_id(dish_id)
        dish.set_name(row["name"])
        dish.set_price(row["price"])
        dish.set_is_active(row["is_active"])

        # dish = Dish(
        #     dish_id=row["dish_id"],
        #     name=row["name"],
        #     price=row["price"],
        #     is_active=row["is_active"],
        # )

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
        #TODO: does the dish need to be active?
        query = sql.SQL(
            "UPDATE Dishs SET price = {price} WHERE dish_id = {id} AND is_active = true"
        ).format(price=sql.Literal(price), id=sql.Literal(dish_id))

        rows, _ = conn.execute(query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
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
        conn = Connector.DBConnector()        # Update the dish active status
        query = sql.SQL(
            "UPDATE Dishs SET is_active = {active} WHERE dish_id = {id}"
        ).format(active=sql.Literal(is_active), id=sql.Literal(dish_id))

        rows_affected,_ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
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
        conn = Connector.DBConnector()        # Update the order with the customer_id
        query = sql.SQL(
            "INSERT INTO CustomerOrders(cust_id, order_id) VALUES({cust_id}, {order_id})"
        ).format(cust_id=sql.Literal(customer_id), order_id=sql.Literal(order_id))

        rows_affected , _= conn.execute(query)
        
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS

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

        conn = Connector.DBConnector()        # Check if the order exists and get the associated customer
        query = sql.SQL("SELECT CO.cust_id, C.full_name, C.phone, C.age \
                FROM CustomerOrders CO \
                JOIN Customers C ON CO.cust_id = C.cust_id \
                WHERE CO.order_id = {id}"
        ).format(id=sql.Literal(order_id))

        rows_affected, result = conn.execute(query)
        
        # If no customer found for this order
        if result.isEmpty() or rows_affected == 0:
            return BadCustomer()

        row = result[0]
        customer = Customer()
        customer.set_cust_id(row["cust_id"])
        customer.set_full_name(row["full_name"])
        customer.set_phone(row["phone"])
        customer.set_address(row["age"])

        # customer = Customer(
        #     cust_id=row["cust_id"],
        #     full_name=row["full_name"],
        #     phone=row["phone"],
        #     age=row["age"],
        # )

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
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO DishOrders (order_id, dish_id, amount, price) 
            VALUES (
                {oid}, 
                {did}, 
                {amt}, 
                (SELECT price from Dishs WHERE dish_id = {did} AND is_active = true)
            )
        """).format(
            oid=sql.Literal(order_id), 
            did=sql.Literal(dish_id), 
            amt=sql.Literal(amount)
        )
        rows_affected, _ = conn.execute(query)
        conn.commit()
        if rows_affected == 1:
            return ReturnValue.OK
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
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
        conn = Connector.DBConnector()
        query = sql.SQL("""
            DELETE FROM DishOrders 
            WHERE order_id = {o_id} AND dish_id = {d_id}
        """).format(
            o_id=sql.Literal(order_id),
            d_id=sql.Literal(dish_id)
        )
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        conn.commit()
        return ReturnValue.OK
    except Exception:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_all_order_items(order_id: int) -> List[OrderDish]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM DishOrders WHERE order_id={o_id} ORDER BY dish_id ASC").format(o_id=sql.Literal(order_id))
        rows_affected, result = conn.execute(query)
        if rows_affected == 0:
            return []
        else:
            o_list = []
            for row in result:
                OD = OrderDish()
                OD.set_dish_id(row['dish_id'])
                OD.set_price(row['price'])
                OD.set_amount(row['amount'])
                o_list.append(OD)
            return o_list
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


def customer_rated_dish(cust_id: int, dish_id: int, rating: int) -> ReturnValue:
    conn = None
    if rating < 1 or rating > 5:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Ratings (cust_id, dish_id, rating) VALUES ({c_id},{d_id},{r})").format(cid=sql.Literal(cust_id), did=sql.Literal(dish_id), r=sql.Literal(rating))
        rows_affected, _ = conn.execute(query)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def customer_deleted_rating_on_dish(cust_id: int, dish_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Ratings WHERE cust_id={c_id} AND dish_id={d_id}").format(c_id=sql.Literal(cust_id), d_id=sql.Literal(dish_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS

        conn.commit()
        return ReturnValue.OK

    except Exception:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_all_customer_ratings(cust_id: int) -> List[Tuple[int, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Ratings WHERE cust_id = {c_id} ORDER BY dish_id ASC").format(c_id=sql.Literal(cust_id))
        rows_affected, result = conn.execute(query)
        r_list = []
        if rows_affected == 0:
            return []
        for row in result:
            r_list.append((row['dish_id'], row['rating']))
        return r_list
    except Exception:
        return []
    finally:
        if conn:
            conn.close()





# ---------------------------------- BASIC API: ----------------------------------

# Basic API


def get_order_total_price(order_id: int) -> float:
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT totalPrice FROM totalPricePerOrder WHERE order_id = {o_id}").format(o_id=sql.Literal(order_id))
    rows_affected, result = conn.execute(query)
    total_price = result[0]['totalPrice']
    conn.close()
    return float(total_price)


def get_customers_spent_max_avg_amount_money() -> List[int]:
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT CO.cust_id "
                    "FROM CustomerOrders CO, totalPricePerOrder TP "
                    "WHERE CO.order_id = TP.order_id "
                    "GROUP BY cust_id "
                    "HAVING AVG(totalPrice) >= ALL (SELECT AVG(totalPrice) "
                    "FROM CustomerOrders CO, totalPricePerOrder TP "
                    "WHERE CO.order_id = TP.order_id GROUP BY cust_id) "
                    "ORDER BY cust_id ASC").format()
    rows_affected, result = conn.execute(query)
    max_list = []
    for row in result:
        max_list.append(row['cust_id'])
    conn.close()
    return max_list


def get_most_ordered_dish_in_period(start: datetime, end: datetime) -> Dish:
    conn = None
    try:
        # Check if inputs are valid
        if start is None or end is None or start > end:
            return BadDish()

        conn = Connector.DBConnector()

        # Find the dish with the highest total amount ordered in the given period
        query = sql.SQL(            """
            WITH DishAmounts AS (
                SELECT
                    od.dish_id,
                    SUM(od.amount) AS total_amount
                FROM 
                    DishOrders od
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
                    DishAmounts            )
            SELECT 
                d.*
            FROM 
                Dishs d
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
        if cust_id <= 0:
            return False
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT DISTINCT cust_id "
            "FROM CustomerOrders AS CO, DishOrders AS DO, SortRatingsDesc AS SR "
            "WHERE CO.order_id = DO.order_id AND DO.dish_id = SR.dish_id AND CO.cust_id = {c_id}"
        ).format(c_id=sql.Literal(cust_id))

        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return False
        return True

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
            """            SELECT DISTINCT r.cust_id
            FROM Ratings r
            WHERE NOT EXISTS (
                SELECT 1
                FROM Orders o
                JOIN DishOrders od ON o.order_id = od.order_id
                WHERE o.cust_id = r.cust_id AND od.dish_id = r.dish_id
            )
            ORDER BY r.cust_id
        """
        )

        rows_affected, result = conn.execute(query)

        # If no customers found
        if result.isEmpty():
            return []        # Extract customer IDs
        customer_ids = []
        for row in result:
            customer_ids.append(row["cust_id"])

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
                    LAG(d.price) OVER (PARTITION BY od.dish_id ORDER BY o.date) AS prev_price                FROM
                    DishOrders od                JOIN
                    Orders o ON od.order_id = o.order_id
                JOIN
                    Dishs d ON od.dish_id = d.dish_id
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
                    Orders o                JOIN
                    DishOrders od ON o.order_id = od.order_id
                JOIN
                    Dishs d ON od.dish_id = d.dish_id
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
        query = sql.SQL("SELECT * FROM Customers WHERE cust_id = {id}").format(
            id=sql.Literal(cust_id)
        )
        rows_affected, result = conn.execute(query)

        if result.isEmpty():
            return []

        # Find dishes that similar customers ordered but this customer hasn't
        # Two customers are similar if they both rated the same dish with the same rating at least once
        query = sql.SQL(
            """            WITH SimilarCustomers AS (
                -- Customers who gave the same rating to the same dish as our customer
                SELECT DISTINCT r2.cust_id
                FROM Ratings r1
                JOIN Ratings r2 ON r1.dish_id = r2.dish_id AND r1.rating = r2.rating
                WHERE r1.cust_id = {cust_id} AND r2.cust_id != {cust_id}
            ),OrderedByOthers AS (
                -- Dishes ordered by similar customers
                SELECT DISTINCT od.dish_id
                FROM DishOrders od
                JOIN Orders o ON od.order_id = o.order_id
                WHERE o.cust_id IN (SELECT cust_id FROM SimilarCustomers)
            ),
            OrderedByCustomer AS (
                -- Dishes already ordered by our customer
                SELECT DISTINCT od.dish_id
                FROM DishOrders od
                JOIN Orders o ON od.order_id = o.order_id
                WHERE o.cust_id = {cust_id}
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
