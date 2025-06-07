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
            """
            CREATE TABLE Customers (
                cust_id INTEGER PRIMARY KEY CHECK(cust_id > 0),
                full_name TEXT NOT NULL,
                age INTEGER NOT NULL CHECK(age BETWEEN 18 AND 120),
                phone TEXT NOT NULL CHECK(LENGTH(phone) = 10)
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE Orders (
                order_id INTEGER PRIMARY KEY CHECK(order_id > 0),
                date TIMESTAMP NOT NULL,
                delivery_fee DECIMAL NOT NULL CHECK(delivery_fee >= 0),
                delivery_address TEXT NOT NULL CHECK(LENGTH(delivery_address) >= 5)
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE Dishes (
                dish_id INTEGER PRIMARY KEY CHECK(dish_id > 0),
                name TEXT NOT NULL CHECK(LENGTH(name) >= 4),
                price DECIMAL NOT NULL CHECK(price > 0),
                is_active BOOLEAN NOT NULL
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE CustomerOrders (
                order_id INTEGER,
                cust_id INTEGER,
                FOREIGN KEY (cust_id) REFERENCES Customers (cust_id) ON DELETE CASCADE,
                FOREIGN KEY (order_id) REFERENCES Orders (order_id) ON DELETE CASCADE,
                PRIMARY KEY (order_id)
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE DishOrders (
                order_id INTEGER,
                dish_id INTEGER,
                amount INTEGER NOT NULL CHECK(amount >= 0),
                price DECIMAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES Orders (order_id) ON DELETE CASCADE,
                FOREIGN KEY (dish_id) REFERENCES Dishes (dish_id) ON DELETE CASCADE,
                PRIMARY KEY (order_id, dish_id)
            )
        """
        )

        conn.execute(
            """
            CREATE TABLE Ratings (
                cust_id INTEGER,
                dish_id INTEGER,
                rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
                FOREIGN KEY (cust_id) REFERENCES Customers (cust_id) ON DELETE CASCADE,
                FOREIGN KEY (dish_id) REFERENCES Dishes (dish_id) ON DELETE CASCADE,
                PRIMARY KEY (cust_id, dish_id)
            )
        """
        )

        conn.execute(
            """
            CREATE VIEW totalPricePerOrder AS
            SELECT 
                O.order_id AS order_id,
                (COALESCE(SUM(D.amount * D.price), 0) + O.delivery_fee) AS total_price
            FROM DishOrders D
            RIGHT OUTER JOIN Orders O ON O.order_id = D.order_id
            GROUP BY O.order_id, O.delivery_fee
        """
        )

        conn.execute(
            """
            CREATE VIEW sortRatingsDesc AS
            SELECT 
                D.dish_id,
                COALESCE(AVG(DR.rating), 3) AS avg_rating
            FROM Ratings DR
            RIGHT OUTER JOIN Dishes D ON D.dish_id = DR.dish_id
            GROUP BY D.dish_id
            ORDER BY avg_rating DESC, D.dish_id ASC
            LIMIT 5
        """
        )

        conn.execute(
            """
            CREATE VIEW comparedPrices AS
            SELECT 
                DO1.dish_id,
                DO1.price,
                (AVG(DO1.amount) * DO1.price) AS avg_price
            FROM DishOrders DO1
            GROUP BY DO1.dish_id, DO1.price
            HAVING DO1.price <= (SELECT D.price FROM Dishes D WHERE D.dish_id = DO1.dish_id)
        """
        )

        conn.execute(
            """
            CREATE VIEW similarCustomers AS
            WITH RECURSIVE AUX1(C1, C2) AS (
                SELECT 
                    A.cust_id AS C1,
                    B.cust_id AS C2
                FROM Ratings AS A, Ratings AS B
                WHERE A.dish_id = B.dish_id
                  AND A.rating > 3
                  AND B.rating > 3
                UNION
                SELECT 
                    A.cust_id AS C1, 
                    B.cust_id AS C2
                FROM Ratings AS A, Ratings AS B, AUX1
                WHERE A.cust_id = AUX1.C1
                  AND B.cust_id != AUX1.C1
                  AND B.cust_id != AUX1.C2
                  AND A.rating > 3 
                  AND B.rating > 3
                  AND A.dish_id = B.dish_id
            )
            SELECT * FROM AUX1
        """
        )

        conn.execute(
            """
            CREATE VIEW monthlyOrders AS
            SELECT 
                EXTRACT(MONTH FROM o.date) AS month,
                EXTRACT(YEAR FROM o.date) AS year,
                o.order_id,
                o.delivery_fee,
                SUM(od.amount * d.price) AS dishes_total
            FROM Orders o
            JOIN DishOrders od ON o.order_id = od.order_id
            JOIN Dishes d ON od.dish_id = d.dish_id
            GROUP BY month, year, o.order_id, o.delivery_fee
        """
        )

        conn.execute(
            """
            CREATE VIEW monthlyProfit AS
            SELECT 
                month,
                year,
                SUM(delivery_fee + dishes_total) AS monthly_profit
            FROM monthlyOrders
            GROUP BY month, year
            ORDER BY year, month
        """
        )

        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.database_ini_ERROR as e:
        print(e)
    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)
    except Exception as e:
        if conn:
            conn.rollback()
        print(e)
    finally:
        if conn:
            conn.close()


def clear_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("""DELETE FROM Ratings""")
        conn.execute("""DELETE FROM DishOrders""")
        conn.execute("""DELETE FROM CustomerOrders""")
        conn.execute("""DELETE FROM Dishes""")
        conn.execute("""DELETE FROM Orders""")
        conn.execute("""DELETE FROM Customers""")
        
        conn.commit()
    except DatabaseException as e:
        if conn:
            conn.rollback()
        print(e)
    finally:
        if conn:
            conn.close()


def drop_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("""DROP VIEW IF EXISTS monthlyProfit""")
        conn.execute("""DROP VIEW IF EXISTS monthlyOrders""")
        conn.execute("""DROP VIEW IF EXISTS similarCustomers""")
        conn.execute("""DROP VIEW IF EXISTS comparedPrices""")
        conn.execute("""DROP VIEW IF EXISTS sortRatingsDesc""")
        conn.execute("""DROP VIEW IF EXISTS totalPricePerOrder""")

        conn.execute("""DROP TABLE IF EXISTS Ratings""")
        conn.execute("""DROP TABLE IF EXISTS DishOrders""")
        conn.execute("""DROP TABLE IF EXISTS CustomerOrders""")
        conn.execute("""DROP TABLE IF EXISTS Dishes""")
        conn.execute("""DROP TABLE IF EXISTS Orders""")
        conn.execute("""DROP TABLE IF EXISTS Customers""")
        
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(e)
    finally:
        if conn:
            conn.close()


# CRUD API


def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            INSERT INTO Customers (cust_id, full_name, phone, age)
            VALUES ({id}, {name}, {phone}, {age})
        """
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
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT *
            FROM Customers
            WHERE cust_id = {id}
        """
        ).format(id=sql.Literal(customer_id))

        rows_affected, result = conn.execute(query)
        
        if rows_affected == 0 or result.isEmpty():
            return BadCustomer()
        
        row = result[0]
        customer = Customer(
            cust_id=row["cust_id"],
            full_name=row["full_name"],
            phone=row["phone"],
            age=row["age"],
        )
        
        return customer
    except Exception:
        if conn:
            conn.rollback()
        return BadCustomer()
    finally:
        if conn:
            conn.close()


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            DELETE FROM Customers 
            WHERE cust_id = {id}
        """
        ).format(id=sql.Literal(customer_id))

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


def add_order(order: Order) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            INSERT INTO Orders (order_id, date, delivery_fee, delivery_address)
            VALUES ({id}, {date_time}, {fee}, {address})
        """
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
    except Exception:
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
        query = sql.SQL(
            """
            SELECT * 
            FROM Orders 
            WHERE order_id = {id}
        """
        ).format(id=sql.Literal(order_id))

        rows_affected, result = conn.execute(query)
        
        if rows_affected == 0 or result.isEmpty():
            return BadOrder()
        
        row = result[0]
        order = Order(
            order_id=row["order_id"],
            date=row["date"],
            delivery_fee=float(row["delivery_fee"]),
            delivery_address=row["delivery_address"],
        )
        
        return order
    except Exception:
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
        query = sql.SQL(
            """
            DELETE FROM Orders 
            WHERE order_id = {id}
        """
        ).format(id=sql.Literal(order_id))

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


def add_dish(dish: Dish) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            INSERT INTO Dishes (dish_id, name, price, is_active) 
            VALUES ({id}, {name}, {price}, {active})
        """
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
    except Exception:
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
        query = sql.SQL(
            """
            SELECT * 
            FROM Dishes 
            WHERE dish_id = {id}
        """
        ).format(id=sql.Literal(dish_id))

        rows_affected, result = conn.execute(query)
        
        if rows_affected == 0 or result.isEmpty():
            return BadDish()
        
        row = result[0]
        dish = Dish(
            dish_id=row["dish_id"],
            name=row["name"],
            price=float(row["price"]),
            is_active=row["is_active"],
        )
        
        return dish
    except Exception:
        if conn:
            conn.rollback()
        return BadDish()
    finally:
        if conn:
            conn.close()


def update_dish_price(dish_id: int, price: float) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            UPDATE Dishes 
            SET price = {price} 
            WHERE dish_id = {id} 
              AND is_active = true
        """
        ).format(price=sql.Literal(price), id=sql.Literal(dish_id))

        rows_affected, _ = conn.execute(query)
        
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
            
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def update_dish_active_status(dish_id: int, is_active: bool) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            UPDATE Dishes 
            SET is_active = {active} 
            WHERE dish_id = {id}
        """
        ).format(active=sql.Literal(is_active), id=sql.Literal(dish_id))

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


def customer_placed_order(customer_id: int, order_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            INSERT INTO CustomerOrders (cust_id, order_id) 
            VALUES ({cust_id}, {order_id})
        """
        ).format(cust_id=sql.Literal(customer_id), order_id=sql.Literal(order_id))

        _ = conn.execute(query)
        
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except Exception:
        if conn:
            conn.rollback()
        return ReturnValue.ERROR
    finally:
        if conn:
            conn.close()


def get_customer_that_placed_order(order_id: int) -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
                SELECT 
                    CO.cust_id, 
                    C.full_name, 
                    C.phone, 
                    C.age 
                FROM CustomerOrders CO 
                JOIN Customers C ON CO.cust_id = C.cust_id 
                WHERE CO.order_id = {id}"""
        ).format(id=sql.Literal(order_id))

        rows_affected, result = conn.execute(query)
        
        if rows_affected == 0 or result.isEmpty():
            return BadCustomer()
        
        row = result[0]
        customer = Customer(
            cust_id=row["cust_id"],
            full_name=row["full_name"],
            phone=row["phone"],
            age=row["age"],
        )
        
        return customer
    except Exception:
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
        query = sql.SQL(
            """
            INSERT INTO DishOrders (order_id, dish_id, amount, price) 
            VALUES (
                {oid}, 
                {did}, 
                {amt}, 
                (SELECT price FROM Dishes WHERE dish_id = {did} AND is_active = true)
            )
        """
        ).format(
            oid=sql.Literal(order_id), did=sql.Literal(dish_id), amt=sql.Literal(amount)
        )
        _ = conn.execute(query)
        
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
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


def order_does_not_contain_dish(order_id: int, dish_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            DELETE FROM DishOrders 
            WHERE order_id = {o_id} 
              AND dish_id = {d_id}
        """
        ).format(o_id=sql.Literal(order_id), d_id=sql.Literal(dish_id))
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
        query = sql.SQL(
            """
            SELECT * 
            FROM DishOrders 
            WHERE order_id = {o_id} 
            ORDER BY dish_id ASC
        """
        ).format(o_id=sql.Literal(order_id))
        _, result = conn.execute(query)
        
        customer_orders_list = []
        for row in result:
            order_dish = OrderDish(
                dish_id=row["dish_id"], amount=row["amount"], price=float(row["price"])
            )
            customer_orders_list.append(order_dish)
        
        return customer_orders_list
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


def customer_rated_dish(cust_id: int, dish_id: int, rating: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            INSERT INTO Ratings (cust_id, dish_id, rating) 
            VALUES ({c_id}, {d_id}, {r})
        """
        ).format(
            c_id=sql.Literal(cust_id), d_id=sql.Literal(dish_id), r=sql.Literal(rating)
        )
        _ = conn.execute(query)
        
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
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
        query = sql.SQL(
            """
            DELETE FROM Ratings 
            WHERE cust_id = {c_id} 
              AND dish_id = {d_id}
        """
        ).format(c_id=sql.Literal(cust_id), d_id=sql.Literal(dish_id))
        rows_affected, _ = conn.execute(query)
        
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS

        conn.commit()
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
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
        query = sql.SQL(
            """
            SELECT * 
            FROM Ratings 
            WHERE cust_id = {c_id} 
            ORDER BY dish_id ASC
        """
        ).format(c_id=sql.Literal(cust_id))
        _, result = conn.execute(query)
        
        ratings_list = []
        for row in result:
            ratings_list.append((row["dish_id"], row["rating"]))
        
        return ratings_list
    except Exception:
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
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT total_price
            FROM totalPricePerOrder 
            WHERE order_id = {o_id}
        """
        ).format(o_id=sql.Literal(order_id))
        rows_affected, result = conn.execute(query)
        
        if rows_affected == 0 or result.isEmpty():
            return 0.0
            
        total_price = float(result[0]["total_price"])
        
        return total_price
    except Exception:
        if conn:
            conn.rollback()
        return 0.0
    finally:
        if conn:
            conn.close()


def get_customers_spent_max_avg_amount_money() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT CO.cust_id 
            FROM CustomerOrders CO
            JOIN totalPricePerOrder TP ON CO.order_id = TP.order_id 
            GROUP BY CO.cust_id 
            HAVING AVG(TP.total_price) >= ALL (
                SELECT AVG(inner_tp.total_price) 
                FROM CustomerOrders inner_co
                JOIN totalPricePerOrder inner_tp ON inner_co.order_id = inner_tp.order_id 
                GROUP BY inner_co.cust_id
            ) 
            ORDER BY CO.cust_id ASC
        """
        )
        _, result = conn.execute(query)
        
        max_customer_list = []
        for row in result:
            max_customer_list.append(row["cust_id"])
        
        return max_customer_list
    except Exception:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_most_ordered_dish_in_period(start: datetime, end: datetime) -> Dish:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            WITH DishAmounts AS (
                SELECT 
                    od.dish_id,
                    SUM(od.amount) AS total_amount
                FROM DishOrders od
                JOIN Orders o ON od.order_id = o.order_id
                WHERE o.date >= {start} AND o.date <= {end}
                GROUP BY od.dish_id
            ),
            MaxAmount AS (
                SELECT MAX(total_amount) AS max_total_amount
                FROM DishAmounts
            )
            SELECT d.*
            FROM Dishes d
            JOIN DishAmounts da ON d.dish_id = da.dish_id
            JOIN MaxAmount ma ON da.total_amount = ma.max_total_amount
            ORDER BY d.dish_id
            LIMIT 1
        """
        ).format(start=sql.Literal(start), end=sql.Literal(end))

        rows_affected, result = conn.execute(query)
        

        if rows_affected == 0 or result.isEmpty():
            return BadDish()
        
        row = result[0]
        dish = Dish(
            dish_id=row["dish_id"],
            name=row["name"],
            price=float(row["price"]),
            is_active=row["is_active"],
        )
        
        return dish
    except Exception:
        if conn:
            conn.rollback()
        return BadDish()
    finally:
        if conn:
            conn.close()


def did_customer_order_top_rated_dishes(cust_id: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT DISTINCT CO.cust_id 
            FROM CustomerOrders AS CO
            JOIN DishOrders AS D ON CO.order_id = D.order_id
            JOIN sortRatingsDesc AS SR ON D.dish_id = SR.dish_id
            WHERE CO.cust_id = {c_id}
        """
        ).format(c_id=sql.Literal(cust_id))

        rows_affected, _ = conn.execute(query)
        
        return rows_affected > 0
    except Exception:
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
        query = sql.SQL(
            """
            SELECT DISTINCT C.cust_id 
            FROM Customers AS C 
            JOIN Ratings AS R ON R.cust_id = C.cust_id
            JOIN (
                SELECT D.dish_id, COALESCE(AVG(DR.rating), 3) AS avg_rating 
                FROM Ratings DR 
                RIGHT OUTER JOIN Dishes D ON D.dish_id = DR.dish_id 
                GROUP BY D.dish_id 
                ORDER BY avg_rating ASC, D.dish_id ASC
                LIMIT 5
            ) AS RA ON R.dish_id = RA.dish_id
            WHERE R.rating < 3
              AND R.dish_id NOT IN (
                SELECT D.dish_id 
                FROM CustomerOrders AS CO
                JOIN DishOrders AS D ON CO.order_id = D.order_id
                WHERE CO.cust_id = C.cust_id
              )
            ORDER BY C.cust_id
        """
        )

        _, result = conn.execute(query)
        
        customer_ids = []
        for row in result:
            customer_ids.append(row["cust_id"])
        
        return customer_ids
    except Exception:
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
        query = sql.SQL(
            """
            SELECT D.dish_id 
            FROM Dishes D 
            WHERE D.is_active = true 
              AND (D.dish_id, D.price) IN (
                  SELECT dish_id, price 
                  FROM comparedPrices
              ) 
              AND (
                  SELECT avg_price 
                  FROM comparedPrices 
                  WHERE dish_id = D.dish_id AND price = D.price
              ) < (
                  SELECT MAX(avg_price) 
                  FROM comparedPrices 
                  WHERE dish_id = D.dish_id
              ) 
              AND (
                  SELECT COUNT(*) 
                  FROM comparedPrices 
                  WHERE dish_id = D.dish_id
              ) >= 2 
            ORDER BY D.dish_id ASC
        """
        )
        _, result = conn.execute(query)
        
        dish_id_list = []
        for row in result:
            dish_id_list.append(row["dish_id"])
        
        return dish_id_list
    except Exception:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_cumulative_profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            WITH RECURSIVE months(month_num) AS (
                SELECT 1
                UNION ALL
                SELECT month_num + 1 
                FROM months 
                WHERE month_num < 12
            ),
            monthly_profits_for_year AS (
                SELECT 
                    month, 
                    COALESCE(SUM(monthly_profit), 0) AS profit
                FROM monthlyProfit
                WHERE year = {year}
                GROUP BY month
            ),
            all_months AS (
                SELECT 
                    m.month_num, 
                    COALESCE(mp.profit, 0) AS monthly_profit
                FROM months m
                LEFT JOIN monthly_profits_for_year mp ON m.month_num = mp.month
            ),
            cumulative_profits AS (
                SELECT 
                    month_num,
                    SUM(monthly_profit) OVER (ORDER BY month_num) AS cumulative_profit
                FROM all_months
            )
            SELECT 
                month_num AS month, 
                cumulative_profit 
            FROM cumulative_profits 
            ORDER BY month DESC
        """
        ).format(year=sql.Literal(year))

        _, result = conn.execute(query)
        
        monthly_profits = []
        for row in result:
            monthly_profits.append((row["month"], float(row["cumulative_profit"])))
        
        return monthly_profits
    except Exception:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()


def get_potential_dish_recommendations(cust_id: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            """
            SELECT RA.dish_id AS rec 
            FROM similarCustomers AS SC 
            JOIN Ratings AS RA ON (SC.C1 = {c_id} AND SC.C2 = RA.cust_id) 
            WHERE RA.rating > 3 
            EXCEPT (
                SELECT D.dish_id AS rec 
                FROM CustomerOrders AS CO 
                JOIN DishOrders AS D ON CO.order_id = D.order_id 
                WHERE CO.cust_id = {c_id}
            ) 
            ORDER BY rec ASC
        """
        ).format(c_id=sql.Literal(cust_id))

        _, result = conn.execute(query)
        
        recommended_dish_list = []
        for row in result:
            recommended_dish_list.append(row["rec"])
        
        return recommended_dish_list
    except Exception:
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            conn.close()
