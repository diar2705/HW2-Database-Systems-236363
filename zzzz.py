import calendar
import concurrent.futures
from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime
from psycopg2.sql import SQL

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
        conn.execute("""
            CREATE TABLE Customers(
                cust_id INTEGER PRIMARY KEY, 
                full_name TEXT NOT NULL, 
                age INTEGER NOT NULL, 
                phone TEXT NOT NULL, 
                CHECK (cust_id > 0), 
                CHECK (age >= 18), 
                CHECK (age <= 120), 
                CHECK (LENGTH(phone) = 10)
            )
        """)
        
        conn.execute("""
            CREATE TABLE Orders(
                order_id INTEGER PRIMARY KEY, 
                date timestamp NOT NULL, 
                delivery_fee DECIMAL NOT NULL, 
                delivery_address TEXT NOT NULL, 
                CHECK (order_id > 0), 
                CHECK (delivery_fee >= 0), 
                CHECK (LENGTH(delivery_address) >= 5)
            )
        """)
        
        conn.execute("""
            CREATE TABLE Dish(
                dish_id INTEGER PRIMARY KEY CHECK(dish_id > 0), 
                name TEXT NOT NULL CHECK (LENGTH(name) >= 4), 
                price DECIMAL NOT NULL CHECK (price > 0), 
                is_active BOOLEAN NOT NULL
            )
        """)
        
        conn.execute("""
            CREATE TABLE CustomerOrders(
                cust_id INTEGER REFERENCES Customers(cust_id) ON DELETE CASCADE, 
                order_id INTEGER REFERENCES Orders(order_id) ON DELETE CASCADE, 
                PRIMARY KEY (order_id)
            )
        """)
        
        conn.execute("""
            CREATE TABLE DishOrders (
                order_id INTEGER REFERENCES Orders(order_id) ON DELETE CASCADE, 
                dish_id INTEGER REFERENCES Dish(dish_id) ON DELETE CASCADE, 
                amount INTEGER NOT NULL CHECK (amount >= 0), 
                price DECIMAL NOT NULL, 
                PRIMARY KEY (order_id, dish_id)
            )
        """)
        
        conn.execute("""
            CREATE TABLE Ratings(
                cust_id INTEGER REFERENCES Customers(cust_id) ON DELETE CASCADE, 
                dish_id INTEGER REFERENCES Dish(dish_id) ON DELETE CASCADE, 
                rating INTEGER NOT NULL, 
                CHECK (rating >= 1), 
                CHECK (rating <= 5), 
                PRIMARY KEY (cust_id, dish_id)
            )
        """)
        
        conn.execute("""
            CREATE VIEW totalPricePerOrder AS (
                SELECT 
                    O.order_id AS order_id, 
                    (COALESCE(SUM(D.amount * D.price), 0) + O.delivery_fee) AS totalPrice 
                FROM 
                    DishOrders D 
                    RIGHT OUTER JOIN Orders O ON O.order_id = D.order_id 
                GROUP BY 
                    O.order_id, O.delivery_fee
            )
        """)
        
        conn.execute("""
            CREATE VIEW SortedRating AS (
                SELECT 
                    D.dish_id, 
                    COALESCE(AVG(R.rating), 3) AS RA 
                FROM 
                    Ratings R 
                    RIGHT OUTER JOIN Dish D ON R.dish_id = D.dish_id 
                GROUP BY 
                    D.dish_id 
                ORDER BY 
                    RA DESC, D.dish_id ASC
            )
        """)
        
        conn.execute("""
            CREATE VIEW ReversedSortedRating AS (
                SELECT 
                    D.dish_id, 
                    COALESCE(AVG(R.rating), 3) AS RA 
                FROM 
                    Ratings R 
                    RIGHT OUTER JOIN Dish D ON R.dish_id = D.dish_id 
                GROUP BY 
                    D.dish_id 
                ORDER BY 
                    RA ASC, D.dish_id ASC
            )
        """)
        
        conn.execute("""
            CREATE VIEW ValidPrices AS (
                SELECT 
                    O.dish_id, 
                    O.price, 
                    (AVG(O.amount) * O.price) AS averageProfit 
                FROM 
                    dishOrders O 
                GROUP BY 
                    O.dish_id, O.price 
                HAVING 
                    O.price <= (SELECT D.price FROM Dish D WHERE D.dish_id = O.dish_id)
            )
        """)
        
        conn.execute("""
            CREATE VIEW MonthlyProfit AS (
                SELECT 
                    EXTRACT(MONTH FROM O.date) AS Month, 
                    EXTRACT(YEAR FROM O.date) AS Year, 
                    SUM(totalPrice) AS Profit 
                FROM 
                    Orders AS O 
                    JOIN totalPricePerOrder AS D ON O.order_id = D.order_id 
                GROUP BY 
                    Month, Year 
                ORDER BY 
                    Month DESC
            )
        """)
        
        conn.execute("""
            CREATE VIEW SimilarCustomers AS 
            WITH RECURSIVE SC(C1, C2) AS (
                SELECT 
                    A.cust_id AS C1, 
                    B.cust_id AS C2 
                FROM 
                    Ratings AS A, 
                    Ratings AS B 
                WHERE 
                    A.dish_id = B.dish_id AND A.rating >= 4 AND B.rating >= 4 
                UNION 
                SELECT 
                    B.cust_id AS C1, 
                    SCC.C2 AS C2 
                FROM 
                    Ratings AS A, 
                    Ratings AS B, 
                    SC AS SCC 
                WHERE 
                    A.cust_id = SCC.C1 AND 
                    B.cust_id != SCC.C2 AND 
                    B.cust_id != SCC.C1 AND 
                    A.rating >= 4 AND 
                    B.rating >= 4 AND 
                    A.dish_id = B.dish_id
            ) 
            SELECT * FROM SC
        """)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
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
        conn.execute("DELETE FROM Dish")
    except DatabaseException as e:
        print(e)
    finally:
        conn.close()


def drop_tables() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP VIEW MonthlyProfit")
        conn.execute("DROP VIEW totalPricePerOrder")
        conn.execute("DROP VIEW SortedRating")
        conn.execute("DROP VIEW ReversedSortedRating")
        conn.execute("DROP VIEW ValidPrices")
        conn.execute("DROP VIEW SimilarCustomers")
        conn.execute("DROP TABLE Customers CASCADE")
        conn.execute("DROP TABLE Orders CASCADE")
        conn.execute("DROP TABLE Dish CASCADE")
        conn.execute("DROP TABLE DishOrders CASCADE")
        conn.execute("DROP TABLE CustomerOrders CASCADE")
        conn.execute("DROP TABLE Ratings CASCADE")
    except DatabaseException as e:
        print(e)
    finally:
        conn.close()

# CRUD API

def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO Customers (cust_id, full_name, age, phone) 
            VALUES ({0}, {1}, {2}, {3})
        """).format(
            sql.Literal(customer.get_cust_id()),
            sql.Literal(customer.get_full_name()),
            sql.Literal(customer.get_age()),
            sql.Literal(customer.get_phone())
        )
        rows_affected, _ = conn.execute(query)
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        return ReturnValue.ERROR
    finally:
        conn.close()



def get_customer(customer_id: int) -> Customer:
    conn = None
    rows_affected, customer = 0, Customer()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Customers WHERE cust_id = {id}").format(
            id=sql.Literal(customer_id)
        )
        result = ResultSet()
        rows_affected, result = conn.execute(query)
        if rows_affected == 0:
            return BadCustomer()
            
        customer.set_phone(result[0]['phone'])
        customer.set_address(result[0]['age'])
        customer.set_cust_id(result[0]['cust_id'])
        customer.set_full_name(result[0]['full_name'])
        return customer
    except Exception as e:
        return BadCustomer()
    finally:
        conn.close()


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customers WHERE cust_id={id}").format(id=sql.Literal(customer_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def add_order(order: Order) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO Orders (order_id, date, delivery_fee, delivery_address) 
            VALUES ({0}, {1}, {2}, {3})
        """).format(
            sql.Literal(order.get_order_id()), 
            sql.Literal(order.get_datetime()),
            sql.Literal(order.get_delivery_fee()), 
            sql.Literal(order.get_delivery_address())
        )
        rows_affected, _ = conn.execute(query)
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def get_order(order_id: int) -> Order:
    conn = None
    rows_affected, order = 0, Order()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Orders WHERE order_id={id}").format(id=sql.Literal(order_id))
        result = ResultSet()
        rows_affected, result = conn.execute(query)
        if rows_affected == 0:
            return BadOrder()
        order.set_order_id(result[0]['order_id'])
        order.set_datetime(result[0]['date'])
        order.set_delivery_fee(float(result[0]['delivery_fee']))
        order.set_delivery_address(result[0]['delivery_address'])
        return order
    except Exception as e:
        return BadOrder()
    finally:
        conn.close()

def delete_order(order_id: int) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Orders WHERE order_id={id}").format(id=sql.Literal(order_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def add_dish(dish: Dish) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO Dish (dish_id, name, price, is_active) 
            VALUES ({id}, {n}, {p}, {a})
        """).format(
            id=sql.Literal(dish.get_dish_id()), 
            n=sql.Literal(dish.get_name()),
            p=sql.Literal(dish.get_price()), 
            a=sql.Literal(dish.get_is_active())
        )
        rows_affected, _ = conn.execute(query)
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

def get_dish(dish_id: int) -> Dish:
    conn = None
    rows_affected, dish = 0, Dish()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Dish WHERE dish_id={id}").format(id=sql.Literal(dish_id))
        result = ResultSet()
        rows_affected, result = conn.execute(query)
        if rows_affected == 0:
            return BadDish()
        dish.set_dish_id(dish_id)
        dish.set_name(result[0]['name'])
        dish.set_price(result[0]['price'])
        dish.set_is_active(result[0]['is_active'])
        return dish
    except Exception as e:
        return BadDish()
    finally:
        conn.close()

def update_dish_price(dish_id: int, price: float) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE Dish SET price={p} WHERE dish_id={id} AND is_active=true").format(id=sql.Literal(dish_id), p=sql.Literal(price))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def update_dish_active_status(dish_id: int, is_active: bool) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE Dish SET is_active={a} WHERE dish_id={id}").format(id=sql.Literal(dish_id), a=sql.Literal(is_active))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()


def customer_placed_order(customer_id: int, order_id: int) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO CustomerOrders (cust_id, order_id) VALUES ({cid}, {oid})").format(cid=sql.Literal(customer_id), oid=sql.Literal(order_id))
        rows_affected, _ = conn.execute(query)
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()


def get_customer_that_placed_order(order_id: int) -> Customer:
    conn = None
    rows_affected = 0
    customer = Customer()
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT 
                C.cust_id, 
                D.full_name, 
                D.age, 
                D.phone 
            FROM 
                CustomerOrders C 
                JOIN Customers D ON C.cust_id = D.cust_id 
            WHERE 
                order_id = {oid}
        """).format(oid=sql.Literal(order_id))
        
        rows_affected, result = conn.execute(query)
        if rows_affected == 1:
            customer.set_cust_id(result[0]['cust_id'])
            customer.set_full_name(result[0]['full_name'])
            customer.set_phone(result[0]['phone'])
            customer.set_address(result[0]['age'])
            return customer
        else:
            return BadCustomer()
    except Exception:
        return BadCustomer()
    finally:
        conn.close()

def order_contains_dish(order_id: int, dish_id: int, amount: int) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO DishOrders (order_id, dish_id, amount, price) 
            VALUES (
                {oid}, 
                {did}, 
                {amt}, 
                (SELECT price from Dish WHERE dish_id = {did} AND is_active = true)
            )
        """).format(
            oid=sql.Literal(order_id), 
            did=sql.Literal(dish_id), 
            amt=sql.Literal(amount)
        )
        rows_affected, _ = conn.execute(query)
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


def order_does_not_contain_dish(order_id: int, dish_id: int) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            DELETE FROM DishOrders 
            WHERE order_id = {oid} AND dish_id = {did}
        """).format(
            oid=sql.Literal(order_id), 
            did=sql.Literal(dish_id)
        )
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()




def get_all_order_items(order_id: int) -> List[OrderDish]:
    conn = None
    rows_affected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM DishOrders WHERE order_id={oid} ORDER BY dish_id ASC").format(oid=sql.Literal(order_id))
        rows_affected, result = conn.execute(query)
        if rows_affected == 0:
            return []
        else:
            l = []
            for row in result:
                od = OrderDish()
                od.set_dish_id(row['dish_id'])
                od.set_price(row['price'])
                od.set_amount(row['amount'])
                l.append(od)
            return l
    except Exception:
        return []
    finally:
        conn.close()



def customer_rated_dish(cust_id: int, dish_id: int, rating: int) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Ratings (cust_id, dish_id, rating) VALUES ({cid},{did},{r})").format(cid=sql.Literal(cust_id), did=sql.Literal(dish_id), r=sql.Literal(rating))
        rows_affected, _ = conn.execute(query)
        return ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()

def customer_deleted_rating_on_dish(cust_id: int, dish_id: int) -> ReturnValue:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Ratings WHERE cust_id={0} AND dish_id={1}").format(sql.Literal(cust_id), sql.Literal(dish_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected > 0:
            return ReturnValue.OK
        else:
            return ReturnValue.NOT_EXISTS
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()


def get_all_customer_ratings(cust_id: int) -> List[Tuple[int, int]]:
    conn = None
    rows_affected = 0
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Ratings WHERE cust_id = {cid} ORDER BY dish_id ASC").format(cid=sql.Literal(cust_id))
        rows_affected, result = conn.execute(query)
        l = []
        for row in result:
            l.append((row['dish_id'], row['rating']))
        return l
    except Exception:
        return []
    finally:
        conn.close()


# ---------------------------------- BASIC API: ----------------------------------

# Basic API


def get_order_total_price(order_id: int) -> float:
    conn = None
    rows_affected = 0
    result = ResultSet()
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT totalPrice FROM totalPricePerOrder WHERE order_id = {oid}").format(oid=sql.Literal(order_id))
    rows_affected, result = conn.execute(query)
    total_price = result[0]['totalPrice']
    conn.close()
    return float(total_price)

def get_customers_spent_max_avg_amount_money() -> List[int]:
    conn = None
    rows_affected = 0
    result = ResultSet()
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT C.cust_id FROM CustomerOrders C, totalPricePerOrder T WHERE C.order_id = T.order_id GROUP BY cust_id HAVING AVG(totalPrice) >= ALL (SELECT AVG(totalPrice) FROM CustomerOrders C, totalPricePerOrder T WHERE C.order_id = T.order_id GROUP BY cust_id) ORDER BY cust_id ASC").format()
    rows_affected, result = conn.execute(query)
    l = []
    for row in result:
        l.append(row['cust_id'])
    conn.close()
    return l

def get_most_purchased_dish_among_anonymous_order() -> Dish:
    conn = None
    rows_affected = 0
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT 
                D.dish_id, 
                D.price, 
                D.name, 
                D.is_active 
            FROM 
                Dish as D 
                JOIN (
                    SELECT 
                        O.dish_id, 
                        SUM(O.amount) AS S 
                    FROM 
                        DishOrders as O 
                    WHERE 
                        O.order_id NOT IN (SELECT order_id FROM CustomerOrders) 
                    GROUP BY 
                        O.dish_id 
                    ORDER BY 
                        S DESC, O.dish_id ASC 
                    LIMIT 1
                ) AS T ON D.dish_id = T.dish_id
        """)
        
        d = Dish()
        rows_affected, result = conn.execute(query)
        # assuming there is at least one dish from an anonymous order:
        d.set_dish_id(result[0]['dish_id'])
        d.set_price(float(result[0]['price']))
        d.set_name(result[0]['name'])
        d.set_is_active(result[0]['is_active'])
        return d
    finally:
        conn.close()


def did_customer_order_top_rated_dishes(cust_id: int) -> bool:
    conn = None
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        query = SQL("""
            SELECT DISTINCT cust_id 
            FROM 
                CustomerOrders AS C, 
                DishOrders AS D, 
                (SELECT * FROM SortedRating LIMIT 5) AS R 
            WHERE 
                C.order_id = D.order_id AND 
                D.dish_id = R.dish_id AND 
                C.cust_id = {cid}
        """).format(cid=sql.Literal(cust_id))
        
        rows_affected, _ = conn.execute(query)
        return rows_affected != 0
    finally:
        conn.close()


# ---------------------------------- ADVANCED API: ----------------------------------

# Advanced API


def get_customers_rated_but_not_ordered() -> List[int]:
    conn = None
    rows_affected = 0
    result = ResultSet()
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT DISTINCT C.cust_id FROM (Customers AS C JOIN Ratings as R ON R.cust_id = C.cust_id), (SELECT * FROM ReversedSortedRating LIMIT 5) AS RA WHERE R.dish_id = RA.dish_id AND R.rating < 3 AND R.dish_id NOT IN (SELECT D.dish_id FROM CustomerOrders AS CO, DishOrders AS D WHERE CO.order_id = D.order_id AND CO.cust_id = C.cust_id) ").format()
    rows_affected, result = conn.execute(query)
    l = []
    for row in result:
        l.append(row['cust_id'])
    conn.close()
    return l

def get_non_worth_price_increase() -> List[int]:
    conn = None
    rows_affected = 0
    result = ResultSet()
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT D.dish_id FROM Dish as D WHERE D.is_active=true AND (D.dish_id, D.price) IN (SELECT dish_id, price FROM ValidPrices) AND (SELECT averageProfit FROM ValidPrices WHERE price = D.price AND dish_id = D.dish_id) < (SELECT MAX(averageProfit) FROM ValidPrices WHERE dish_id = D.dish_id) AND (SELECT COUNT(*) FROM ValidPrices WHERE dish_id = D.dish_id)>=2 ORDER BY D.dish_id ASC").format()
    rows_affected, result = conn.execute(query)
    l = []
    for row in result:
        l.append(row['dish_id'])
    conn.close()
    return l

def get_cumulative_profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    rows_affected = 0
    result = ResultSet()
    conn = Connector.DBConnector()
    query = sql.SQL("WITH RECURSIVE CPPM AS (SELECT 1 AS Monthh, COALESCE(SUM(MP.Profit), 0) AS cumulativeProfit FROM MonthlyProfit MP WHERE MP.Year={y} AND MP.Month < 2 UNION (SELECT ((C.Monthh) + 1) AS Monthh, (SELECT COALESCE(SUM(Profit), 0) FROM MonthlyProfit WHERE Year={y} AND Month <= Monthh+1) AS cumulativeProfit FROM MonthlyProfit AS M, CPPM AS C WHERE Monthh < 13 AND Monthh >= C.Monthh GROUP BY C.Monthh)) SELECT * FROM CPPM ORDER BY Monthh DESC").format(y=sql.Literal(year))
    rows_affected, result = conn.execute(query)
    l = []
    for i in range(1,13):
        l.append((result[i]['Monthh'], float(result[i]['cumulativeProfit'])))
    conn.close()
    return l
def get_potential_dish_recommendations(cust_id: int) -> List[int]:
    conn = None
    rows_affected = 0
    result = ResultSet()
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT R.dish_id AS did FROM SimilarCustomers AS S JOIN Ratings AS R ON (S.C1 = {cid} AND S.C2 = R.cust_id) WHERE R.rating >= 4  EXCEPT (SELECT D.dish_id AS did FROM CustomerOrders AS C JOIN DishOrders as D ON C.order_id = D.order_id WHERE C.cust_id = {cid}) ORDER BY did ASC").format(cid=sql.Literal(cust_id))
    rows_affected, result = conn.execute(query)
    l = []
    for row in result:
        l.append((row['did']))
    conn.close()
    return l

