import unittest
import sys
import os

# Add the parent directory to the path so we can import Solution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import Solution as Solution
from Utility.ReturnValue import ReturnValue
from Tests.AbstractTest import AbstractTest
from Business.Customer import Customer, BadCustomer

'''
    Simple test, create one of your own
    make sure the tests' names start with test
'''


class Test(AbstractTest):
    def test_001_add_customer_edge_cases(self) -> None:
        # Test adding a customer with negative ID (should fail with BAD_PARAMS due to CHECK constraint)
        c_negative_id = Customer(-1, 'Test Name', 25, "1234567890")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_negative_id), 'negative ID')
        
        # Test adding a customer with age below 18 (should fail with BAD_PARAMS)
        c_underage = Customer(3, 'Young Person', 17, "1234567890")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_underage), 'age below 18')
        
        # Test adding a customer with age above 120 (should fail with BAD_PARAMS)
        c_overage = Customer(4, 'Old Person', 121, "1234567890")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_overage), 'age above 120')
        
        # Test adding a customer with invalid phone (not 10 digits)
        c_short_phone = Customer(5, 'Short Phone', 30, "12345")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_short_phone), 'phone too short')
        
        c_long_phone = Customer(6, 'Long Phone', 30, "12345678901234")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_long_phone), 'phone too long')
        
        # Test adding a customer that already exists
        c_duplicate = Customer(7, 'Original', 35, "9876543210")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c_duplicate), 'original customer')
        
        c_duplicate_2 = Customer(7, 'Duplicate', 40, "1122334455")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_customer(c_duplicate_2), 'duplicate customer ID')
        
        # Test with null values
        c_null_name = Customer(8, None, 30, "1234567890")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_null_name), 'null name')
        
        c_null_age = Customer(9, 'Null Age', None, "1234567890")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_null_age), 'null age')
        
        c_null_phone = Customer(10, 'Null Phone', 30, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c_null_phone), 'null phone')

    def test_002_get_customer_edge_cases(self) -> None:
        # Setup: Add a customer to retrieve
        c_original = Customer(100, 'Get Test', 30, "1112223333")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c_original), 'add test customer')
        
        # Test getting an existing customer
        retrieved_customer = Solution.get_customer(100)
        self.assertEqual(100, retrieved_customer.get_cust_id(), 'retrieved customer ID')
        self.assertEqual('Get Test', retrieved_customer.get_full_name(), 'retrieved customer name')
        self.assertEqual(30, retrieved_customer.get_age(), 'retrieved customer age')
        self.assertEqual("1112223333", retrieved_customer.get_phone(), 'retrieved customer phone')
        
        # Test getting a non-existent customer
        bad_customer = Solution.get_customer(999)
        self.assertEqual(BadCustomer.__name__, bad_customer.__class__.__name__, 'non-existent customer')
        
        # Test getting a customer with negative ID
        bad_customer_neg = Solution.get_customer(-5)
        self.assertEqual(BadCustomer.__name__, bad_customer_neg.__class__.__name__, 'negative ID customer')
        
        # Test getting a customer after deleting it
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(100), 'delete test customer')
        deleted_customer = Solution.get_customer(100)
        self.assertEqual(BadCustomer.__name__, deleted_customer.__class__.__name__, 'deleted customer')

    def test_003_delete_customer_edge_cases(self) -> None:
        # Setup: Add customers to delete
        c1 = Customer(200, 'Delete Test', 40, "9998887777")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add test customer 1')
        
        c2 = Customer(201, 'Delete Test 2', 41, "9998887778")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c2), 'add test customer 2')
        
        # Test deleting an existing customer
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(200), 'delete existing customer')
        
        # Test deleting a non-existent customer
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_customer(999), 'delete non-existent customer')
        
        # Test deleting a customer with negative ID
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_customer(-5), 'delete with negative ID')
        
        # Test deleting the same customer twice
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(201), 'delete first time')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_customer(201), 'delete second time')
        
        # Test deleting a customer and verifying it's gone
        c3 = Customer(202, 'Delete Verify', 45, "1231231231")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c3), 'add test customer 3')
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(202), 'delete customer 3')
        self.assertEqual(BadCustomer.__name__, Solution.get_customer(202).__class__.__name__, 'verify deleted')

    def test_004_add_order_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Order import Order, BadOrder
        
        # Test adding a valid order
        order1 = Order(300, datetime.now(), 15.50, "5 Main Street, Haifa")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add valid order')
        
        # Test adding an order with negative ID
        order_neg_id = Order(-300, datetime.now(), 10.00, "10 Cedar Street, Tel Aviv")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_order(order_neg_id), 'negative order ID')
        
        # Test adding an order with negative delivery fee
        order_neg_fee = Order(301, datetime.now(), -5.00, "15 Oak Street, Jerusalem")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_order(order_neg_fee), 'negative delivery fee')
        
        # Test adding an order with too short delivery address (less than 5 chars)
        order_short_addr = Order(302, datetime.now(), 10.00, "123")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_order(order_short_addr), 'delivery address too short')
        
        # Test adding an order that already exists
        order_dup = Order(300, datetime.now(), 20.00, "Different Address, Haifa")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_order(order_dup), 'duplicate order ID')
        
        # Test with null values
        order_null_date = Order(303, None, 10.00, "20 Pine Street, Tel Aviv")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_order(order_null_date), 'null date')
        
        order_null_fee = Order(304, datetime.now(), None, "25 Elm Street, Haifa")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_order(order_null_fee), 'null delivery fee')
        
        order_null_addr = Order(305, datetime.now(), 10.00, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_order(order_null_addr), 'null delivery address')
        
        # Test adding an order with zero delivery fee (should be valid)
        order_zero_fee = Order(306, datetime.now(), 0, "30 Maple Street, Jerusalem")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order_zero_fee), 'zero delivery fee')

    def test_005_get_order_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Order import Order, BadOrder
        
        # Setup: Add an order to retrieve
        test_time = datetime.now()
        order1 = Order(400, test_time, 25.50, "40 Test Street, Haifa")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add test order')
        
        # Test getting an existing order
        retrieved_order = Solution.get_order(400)
        self.assertEqual(400, retrieved_order.get_order_id(), 'retrieved order ID')
        self.assertEqual(25.50, retrieved_order.get_delivery_fee(), 'retrieved delivery fee')
        self.assertEqual("40 Test Street, Haifa", retrieved_order.get_delivery_address(), 'retrieved address')
        
        # Test getting a non-existent order
        bad_order = Solution.get_order(999)
        self.assertEqual(BadOrder.__name__, bad_order.__class__.__name__, 'non-existent order')
        
        # Test getting an order with negative ID
        bad_order_neg = Solution.get_order(-5)
        self.assertEqual(BadOrder.__name__, bad_order_neg.__class__.__name__, 'negative ID order')
        
        # Test getting an order after deleting it
        self.assertEqual(ReturnValue.OK, Solution.delete_order(400), 'delete test order')
        deleted_order = Solution.get_order(400)
        self.assertEqual(BadOrder.__name__, deleted_order.__class__.__name__, 'deleted order')

    def test_006_delete_order_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Order import Order, BadOrder
        
        # Setup: Add orders to delete
        order1 = Order(500, datetime.now(), 15.00, "50 Delete Street, Haifa")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add test order 1')
        
        order2 = Order(501, datetime.now(), 17.50, "51 Delete Avenue, Tel Aviv")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add test order 2')
        
        # Test deleting an existing order
        self.assertEqual(ReturnValue.OK, Solution.delete_order(500), 'delete existing order')
        
        # Test deleting a non-existent order
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_order(999), 'delete non-existent order')
        
        # Test deleting an order with negative ID
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_order(-5), 'delete with negative ID')
        
        # Test deleting the same order twice
        self.assertEqual(ReturnValue.OK, Solution.delete_order(501), 'delete first time')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_order(501), 'delete second time')
        
        # Test deleting an order and verifying it's gone
        order3 = Order(502, datetime.now(), 20.00, "52 Delete Boulevard, Jerusalem")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), 'add test order 3')
        self.assertEqual(ReturnValue.OK, Solution.delete_order(502), 'delete order 3')
        self.assertEqual(BadOrder.__name__, Solution.get_order(502).__class__.__name__, 'verify deleted')

    def test_007_add_dish_edge_cases(self) -> None:
        from Business.Dish import Dish, BadDish
        
        # Test adding a valid dish
        dish1 = Dish(600, "Pasta Carbonara", 45.99, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add valid dish')
        
        # Test adding a dish with negative ID
        dish_neg_id = Dish(-600, "Negative ID Dish", 30.50, True)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_dish(dish_neg_id), 'negative dish ID')
        
        # Test adding a dish with too short name (less than 4 chars)
        dish_short_name = Dish(601, "Abc", 25.99, True)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_dish(dish_short_name), 'name too short')
        
        # Test adding a dish with negative or zero price
        dish_neg_price = Dish(602, "Negative Price", -10.00, True)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_dish(dish_neg_price), 'negative price')
        
        dish_zero_price = Dish(603, "Zero Price", 0, True)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_dish(dish_zero_price), 'zero price')
        
        # Test adding a dish that already exists
        dish_dup = Dish(600, "Different Dish", 25.99, False)
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_dish(dish_dup), 'duplicate dish ID')
        
        # Test with null values
        dish_null_name = Dish(604, None, 30.00, True)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_dish(dish_null_name), 'null name')
        
        dish_null_price = Dish(605, "Null Price Dish", None, True)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_dish(dish_null_price), 'null price')
        
        dish_null_active = Dish(606, "Null Active Dish", 35.99, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_dish(dish_null_active), 'null is_active')
        
        # Test adding dish with minimum valid values
        dish_min_valid = Dish(607, "Four", 0.01, False)  # Minimum valid name length and price
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish_min_valid), 'minimum valid values')

    def test_008_get_dish_edge_cases(self) -> None:
        from Business.Dish import Dish, BadDish
        
        # Setup: Add a dish to retrieve
        dish1 = Dish(700, "Test Pizza", 55.99, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add test dish')
        
        # Test getting an existing dish
        retrieved_dish = Solution.get_dish(700)
        self.assertEqual(700, retrieved_dish.get_dish_id(), 'retrieved dish ID')
        self.assertEqual("Test Pizza", retrieved_dish.get_name(), 'retrieved dish name')
        self.assertEqual(55.99, retrieved_dish.get_price(), 'retrieved dish price')
        self.assertEqual(True, retrieved_dish.get_is_active(), 'retrieved dish active status')
        
        # Test getting a non-existent dish
        bad_dish = Solution.get_dish(999)
        self.assertEqual(BadDish.__name__, bad_dish.__class__.__name__, 'non-existent dish')
        
        # Test getting a dish with negative ID
        bad_dish_neg = Solution.get_dish(-5)
        self.assertEqual(BadDish.__name__, bad_dish_neg.__class__.__name__, 'negative ID dish')
        
        # Test getting a dish after updating its price
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(700, 60.99), 'update dish price')
        updated_dish = Solution.get_dish(700)
        self.assertEqual(60.99, updated_dish.get_price(), 'updated price retrieved correctly')
        
        # Test getting a dish after updating its active status
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(700, False), 'update dish active status')
        updated_dish_status = Solution.get_dish(700)
        self.assertEqual(False, updated_dish_status.get_is_active(), 'updated active status retrieved correctly')

    def test_009_update_dish_price_edge_cases(self) -> None:
        from Business.Dish import Dish, BadDish
        
        # Setup: Add dishes to update
        dish_active = Dish(800, "Active Dish", 45.50, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish_active), 'add active dish')
        
        dish_inactive = Dish(801, "Inactive Dish", 50.75, False)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish_inactive), 'add inactive dish')
        
        # Test updating price of an active dish
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(800, 55.25), 'update active dish price')
        updated_dish = Solution.get_dish(800)
        self.assertEqual(55.25, updated_dish.get_price(), 'verify price updated')
        
        # Test updating to a negative price (should fail with BAD_PARAMS)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.update_dish_price(800, -10.00), 'update to negative price')
        still_updated_dish = Solution.get_dish(800)
        self.assertEqual(55.25, still_updated_dish.get_price(), 'price unchanged after invalid update')
        
        # Test updating to zero price (should fail with BAD_PARAMS)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.update_dish_price(800, 0), 'update to zero price')
        
        # Test updating a non-existent dish
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.update_dish_price(999, 30.00), 'update non-existent dish')
        
        # Test updating an inactive dish (should return NOT_EXISTS)
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.update_dish_price(801, 60.00), 'update inactive dish')
        inactive_dish = Solution.get_dish(801)
        self.assertEqual(50.75, inactive_dish.get_price(), 'inactive dish price unchanged')
        
        # Test updating dish with negative ID
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.update_dish_price(-5, 25.00), 'update with negative ID')
        
        # Test changing active status then updating price
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(801, True), 'make inactive dish active')
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(801, 65.25), 'update newly active dish')
        newly_active_dish = Solution.get_dish(801)
        self.assertEqual(65.25, newly_active_dish.get_price(), 'verify price updated after status change')
        
        # Test price update with minimal valid value
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(800, 0.01), 'update to minimum valid price')
        min_price_dish = Solution.get_dish(800)
        self.assertEqual(0.01, min_price_dish.get_price(), 'verify minimum price updated')

    def test_010_update_dish_active_status_edge_cases(self) -> None:
        from Business.Dish import Dish, BadDish
        
        # Setup: Add dishes to update
        dish1 = Dish(900, "Active Status Test", 45.50, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add active dish')
        
        dish2 = Dish(901, "Second Status Test", 50.75, False)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add inactive dish')
        
        # Test updating active dish to inactive
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(900, False), 'update active to inactive')
        updated_dish = Solution.get_dish(900)
        self.assertEqual(False, updated_dish.get_is_active(), 'verify status updated to inactive')
        
        # Test updating inactive dish to active
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(901, True), 'update inactive to active')
        updated_dish2 = Solution.get_dish(901)
        self.assertEqual(True, updated_dish2.get_is_active(), 'verify status updated to active')
        
        # Test updating status to same value (should still return OK)
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(900, False), 'update to same status')
        
        # Test updating a non-existent dish
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.update_dish_active_status(999, True), 'update non-existent dish')
        
        # Test updating dish with negative ID
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.update_dish_active_status(-5, True), 'update with negative ID')
        
        # No direct way to delete dish, so we'll skip the delete test
        # Instead test multiple status updates
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(901, False), 'update back to inactive')
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(901, True), 'update back to active again')
        
        # Test effect of status on price updates
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(900, True), 'make dish active')
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(900, 60.00), 'update price when active')
        
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(900, False), 'make dish inactive again')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.update_dish_price(900, 65.00), 'update price when inactive')

    def test_011_customer_placed_order_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Customer import Customer
        from Business.Order import Order
        
        # Setup: Add customers and orders
        cust1 = Customer(1000, 'Order Test Customer', 30, "1234567890")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add test customer 1')
        
        cust2 = Customer(1001, 'Second Order Test', 35, "9876543210")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust2), 'add test customer 2')
        
        order1 = Order(1000, datetime.now(), 15.00, "100 Order Test Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add test order 1')
        
        order2 = Order(1001, datetime.now(), 20.00, "101 Order Test Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add test order 2')
        
        # Test valid customer placing order
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(1000, 1000), 'valid customer places order')
        
        # Test placing order with non-existent customer
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_placed_order(9999, 1001), 'non-existent customer places order')
        
        # Test placing non-existent order
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_placed_order(1001, 9999), 'customer places non-existent order')
        
        # Test placing order with negative customer ID
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_placed_order(-5, 1001), 'negative ID customer places order')
        
        # Test placing order with negative order ID
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_placed_order(1001, -5), 'customer places negative ID order')
        
        # Test placing the same order twice (already exists)
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(1001, 1001), 'first time placing order')
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.customer_placed_order(1001, 1001), 'second time placing same order')

    def test_012_get_customer_that_placed_order_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Customer import Customer, BadCustomer
        from Business.Order import Order
        
        # Setup: Add customers and orders
        cust1 = Customer(1100, 'Get Order Customer', 40, "5551234567")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add test customer 1')
        
        order1 = Order(1100, datetime.now(), 25.00, "110 Get Order Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add test order 1')
        
        order2 = Order(1101, datetime.now(), 30.00, "111 Get Order Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add test order 2 (unassigned)')
        
        # Connect customer to order
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(1100, 1100), 'connect customer to order')
        
        # Test getting customer for assigned order
        retrieved_customer = Solution.get_customer_that_placed_order(1100)
        self.assertEqual(1100, retrieved_customer.get_cust_id(), 'retrieved correct customer ID')
        self.assertEqual('Get Order Customer', retrieved_customer.get_full_name(), 'retrieved correct customer name')
        
        # Test getting customer for unassigned order
        unassigned_customer = Solution.get_customer_that_placed_order(1101)
        self.assertEqual(BadCustomer.__name__, unassigned_customer.__class__.__name__, 'unassigned order')
        
        # Test getting customer for non-existent order
        nonexistent_customer = Solution.get_customer_that_placed_order(9999)
        self.assertEqual(BadCustomer.__name__, nonexistent_customer.__class__.__name__, 'non-existent order')
        
        # Test getting customer for order with negative ID
        negative_order_customer = Solution.get_customer_that_placed_order(-5)
        self.assertEqual(BadCustomer.__name__, negative_order_customer.__class__.__name__, 'negative order ID')
        
        # Test getting customer after deleting the order
        self.assertEqual(ReturnValue.OK, Solution.delete_order(1100), 'delete order')
        deleted_order_customer = Solution.get_customer_that_placed_order(1100)
        self.assertEqual(BadCustomer.__name__, deleted_order_customer.__class__.__name__, 'deleted order')
        
        # Test getting customer after deleting the customer
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(1100, 1101), 'new customer-order connection')
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(1100), 'delete customer')
        deleted_customer = Solution.get_customer_that_placed_order(1101)
        self.assertEqual(BadCustomer.__name__, deleted_customer.__class__.__name__, 'order with deleted customer')

    def test_013_order_contains_dish_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Dish import Dish
        from Business.Order import Order
        
        # Setup: Add dishes and orders
        dish1 = Dish(1200, "Dish Order Test 1", 45.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add test dish 1')
        
        dish2 = Dish(1201, "Dish Order Test 2", 50.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add test dish 2')
        
        dish3 = Dish(1202, "Inactive Dish", 55.00, False)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), 'add inactive dish')
        
        order1 = Order(1200, datetime.now(), 10.00, "120 Order Dish Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add test order 1')
        
        # Test adding valid dish to order
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(1200, 1200, 2), 'add valid dish to order')
        
        # Test adding dish with quantity of 0 (should be valid)
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(1200, 1201, 0), 'add dish with zero quantity')
        
        # Test adding inactive dish to order (should fail as price is fetched only for active dishes)
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_contains_dish(1200, 1202, 1), 'add inactive dish')
        
        # Test adding dish with negative quantity (should fail)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.order_contains_dish(1200, 1200, -1), 'add dish with negative quantity')
        
        # Test adding the same dish twice (should fail with ALREADY_EXISTS)
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.order_contains_dish(1200, 1200, 3), 'add same dish twice')
        
        # Test adding dish to non-existent order
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_contains_dish(9999, 1200, 1), 'add to non-existent order')
        
        # Test adding non-existent dish to order
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_contains_dish(1200, 9999, 1), 'add non-existent dish')
        
        # Test with negative IDs
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_contains_dish(-5, 1200, 1), 'negative order ID')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_contains_dish(1200, -5, 1), 'negative dish ID')

    def test_014_order_does_not_contain_dish_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Dish import Dish
        from Business.Order import Order
        
        # Setup: Add dishes and orders
        dish1 = Dish(1300, "Remove Dish Test 1", 35.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add test dish 1')
        
        dish2 = Dish(1301, "Remove Dish Test 2", 40.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add test dish 2')
        
        order1 = Order(1300, datetime.now(), 12.00, "130 Remove Dish Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add test order 1')
        
        # Add dishes to order for later removal
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(1300, 1300, 2), 'add dish 1 to order')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(1300, 1301, 1), 'add dish 2 to order')
        
        # Test removing existing dish from order
        self.assertEqual(ReturnValue.OK, Solution.order_does_not_contain_dish(1300, 1300), 'remove existing dish')
        
        # Test removing a dish that's not in the order (but exists in the database)
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_does_not_contain_dish(1300, 1300), 'remove already removed dish')
        
        # Test removing from non-existent order
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_does_not_contain_dish(9999, 1301), 'remove from non-existent order')
        
        # Test removing non-existent dish
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_does_not_contain_dish(1300, 9999), 'remove non-existent dish')
        
        # Test with negative IDs
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_does_not_contain_dish(-5, 1301), 'negative order ID')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_does_not_contain_dish(1300, -5), 'negative dish ID')
        
        # Test removing a dish from order after deleting the order (should return NOT_EXISTS)
        self.assertEqual(ReturnValue.OK, Solution.delete_order(1300), 'delete order')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.order_does_not_contain_dish(1300, 1301), 'remove dish from deleted order')

    def test_015_get_all_order_items_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Dish import Dish
        from Business.Order import Order
        from Business.OrderDish import OrderDish
        
        # Setup: Add dishes and orders
        dish1 = Dish(1400, "Order Items Test 1", 25.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add test dish 1')
        
        dish2 = Dish(1401, "Order Items Test 2", 30.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add test dish 2')
        
        dish3 = Dish(1402, "Order Items Test 3", 35.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), 'add test dish 3')
        
        order1 = Order(1400, datetime.now(), 15.00, "140 Order Items Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add test order 1')
        
        order2 = Order(1401, datetime.now(), 18.00, "141 Order Items Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add test order 2 (empty)')
        
        # Add dishes to order
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(1400, 1400, 2), 'add dish 1 to order')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(1400, 1401, 1), 'add dish 2 to order')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(1400, 1402, 3), 'add dish 3 to order')
        
        # Test getting items from order with multiple dishes
        items = Solution.get_all_order_items(1400)
        self.assertEqual(3, len(items), 'correct number of items')
        
        # Verify the content of returned items
        dish_ids = [item.get_dish_id() for item in items]
        self.assertIn(1400, dish_ids, 'contains dish 1')
        self.assertIn(1401, dish_ids, 'contains dish 2')
        self.assertIn(1402, dish_ids, 'contains dish 3')
        
        # Find dish 1 in the list and check its details
        dish1_item = next(item for item in items if item.get_dish_id() == 1400)
        self.assertEqual(2, dish1_item.get_amount(), 'correct amount for dish 1')
        self.assertEqual(25.00, dish1_item.get_price(), 'correct price for dish 1')
        
        # Test getting items from empty order
        empty_items = Solution.get_all_order_items(1401)
        self.assertEqual(0, len(empty_items), 'empty order has no items')
        
        # Test getting items from non-existent order
        non_existent_items = Solution.get_all_order_items(9999)
        self.assertEqual(0, len(non_existent_items), 'non-existent order has no items')
        
        # Test getting items with negative order ID
        negative_id_items = Solution.get_all_order_items(-5)
        self.assertEqual(0, len(negative_id_items), 'negative ID order has no items')
        
        # Test getting items after removing a dish
        self.assertEqual(ReturnValue.OK, Solution.order_does_not_contain_dish(1400, 1401), 'remove dish from order')
        updated_items = Solution.get_all_order_items(1400)
        self.assertEqual(2, len(updated_items), 'correct number after removal')
        updated_dish_ids = [item.get_dish_id() for item in updated_items]
        self.assertNotIn(1401, updated_dish_ids, 'removed dish not present')
        
        # Test getting items after deleting the order
        self.assertEqual(ReturnValue.OK, Solution.delete_order(1400), 'delete order')
        deleted_order_items = Solution.get_all_order_items(1400)
        self.assertEqual(0, len(deleted_order_items), 'deleted order has no items')

    def test_016_customer_rated_dish_edge_cases(self) -> None:
        from Business.Customer import Customer
        from Business.Dish import Dish
        
        # Setup: Add customers and dishes
        cust1 = Customer(1500, 'Rating Test Customer', 35, "1111222233")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add test customer 1')
        
        cust2 = Customer(1501, 'Rating Test Customer 2', 40, "4444555566")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust2), 'add test customer 2')
        
        dish1 = Dish(1500, "Rating Dish Test", 45.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add test dish 1')
        
        dish2 = Dish(1501, "Rating Dish Test 2", 50.00, False)  # inactive dish
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add inactive dish')
        
        # Test valid rating
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1500, 1500, 5), 'valid 5-star rating')
        
        # Test rating out of range
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_rated_dish(1500, 1501, 0), 'rating too low')
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_rated_dish(1500, 1501, 6), 'rating too high')
        
        # Test rating inactive dish (should still work)
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1500, 1501, 3), 'rate inactive dish')
        
        # Test with non-existent customer
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_rated_dish(9999, 1500, 4), 'non-existent customer')
        
        # Test with non-existent dish
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_rated_dish(1500, 9999, 4), 'non-existent dish')
        
        # Test with negative IDs
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_rated_dish(-5, 1500, 4), 'negative customer ID')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_rated_dish(1500, -5, 4), 'negative dish ID')
        
        # Test rating same dish twice (should update the rating)
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.customer_rated_dish(1500, 1500, 2), 'update previous rating')
        
        # Multiple customers rating same dish
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1501, 1500, 1), 'second customer rating')
        
        # Same customer rating multiple dishes
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1501, 1501, 5), 'customer rates second dish')

    def test_017_customer_deleted_rating_on_dish_edge_cases(self) -> None:
        from Business.Customer import Customer
        from Business.Dish import Dish
        
        # Setup: Add customers and dishes
        cust1 = Customer(1600, 'Delete Rating Customer', 30, "9998887766")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add test customer 1')
        
        cust2 = Customer(1601, 'Delete Rating Customer 2', 32, "5554443322")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust2), 'add test customer 2')
        
        dish1 = Dish(1600, "Delete Rating Dish", 30.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add test dish 1')
        
        dish2 = Dish(1601, "Delete Rating Dish 2", 35.00, False)  # inactive dish
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add inactive dish')
        
        # Add ratings to delete
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1600, 1600, 4), 'add rating 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1600, 1601, 3), 'add rating 2')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1601, 1600, 5), 'add rating 3')
        
        # Test deleting existing rating
        self.assertEqual(ReturnValue.OK, Solution.customer_deleted_rating_on_dish(1600, 1600), 'delete existing rating')
        
        # Test deleting non-existent rating
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_deleted_rating_on_dish(1601, 1601), 'delete non-existent rating')
        
        # Test deleting already deleted rating
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_deleted_rating_on_dish(1600, 1600), 'delete already deleted rating')
        
        # Test deleting with non-existent customer
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_deleted_rating_on_dish(9999, 1600), 'non-existent customer')
        
        # Test deleting with non-existent dish
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_deleted_rating_on_dish(1600, 9999), 'non-existent dish')
        
        # Test with negative IDs
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_deleted_rating_on_dish(-5, 1600), 'negative customer ID')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_deleted_rating_on_dish(1600, -5), 'negative dish ID')
        
        # Test effect of deleting dish on ratings
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(1601), 'delete customer')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_deleted_rating_on_dish(1601, 1600), 'deleted customer rating')

    def test_018_get_all_customer_ratings_edge_cases(self) -> None:
        from Business.Customer import Customer
        from Business.Dish import Dish
        
        # Setup: Add customers and dishes
        cust1 = Customer(1700, 'Get Ratings Customer', 25, "1212121212")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add test customer 1')
        
        cust2 = Customer(1701, 'Get Ratings Customer 2', 28, "3434343434")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust2), 'add test customer 2 (no ratings)')
        
        dish1 = Dish(1700, "Get Ratings Dish 1", 25.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add test dish 1')
        
        dish2 = Dish(1701, "Get Ratings Dish 2", 30.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add test dish 2')
        
        dish3 = Dish(1702, "Get Ratings Dish 3", 35.00, False)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), 'add test dish 3 (inactive)')
        
        # Add ratings for customer 1
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1700, 1700, 5), 'add rating 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1700, 1701, 4), 'add rating 2')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(1700, 1702, 3), 'add rating 3')
        
        # Test getting ratings for customer with multiple ratings
        ratings = Solution.get_all_customer_ratings(1700)
        self.assertEqual(3, len(ratings), 'correct number of ratings')
        
        # Check that ratings are returned as tuples of (dish_id, rating)
        dish_ids = [rating[0] for rating in ratings]
        self.assertIn(1700, dish_ids, 'contains dish 1 rating')
        self.assertIn(1701, dish_ids, 'contains dish 2 rating')
        self.assertIn(1702, dish_ids, 'contains dish 3 rating')
        
        # Find dish 1's rating and verify its value
        dish1_rating = next(rating for rating in ratings if rating[0] == 1700)
        self.assertEqual(5, dish1_rating[1], 'correct rating value for dish 1')
        
        # Test getting ratings for customer with no ratings
        empty_ratings = Solution.get_all_customer_ratings(1701)
        self.assertEqual(0, len(empty_ratings), 'customer with no ratings')
        
        # Test getting ratings for non-existent customer
        non_existent_ratings = Solution.get_all_customer_ratings(9999)
        self.assertEqual(0, len(non_existent_ratings), 'non-existent customer has no ratings')
        
        # Test with negative customer ID
        negative_id_ratings = Solution.get_all_customer_ratings(-5)
        self.assertEqual(0, len(negative_id_ratings), 'negative ID customer has no ratings')
        
        # Test getting ratings after deleting a rating
        self.assertEqual(ReturnValue.OK, Solution.customer_deleted_rating_on_dish(1700, 1701), 'delete one rating')
        updated_ratings = Solution.get_all_customer_ratings(1700)
        self.assertEqual(2, len(updated_ratings), 'correct count after deletion')
        updated_dish_ids = [rating[0] for rating in updated_ratings]
        self.assertNotIn(1701, updated_dish_ids, 'deleted rating not present')
        
        # Test getting ratings after deleting the customer
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(1700), 'delete customer')
        deleted_customer_ratings = Solution.get_all_customer_ratings(1700)
        self.assertEqual(0, len(deleted_customer_ratings), 'deleted customer has no ratings')

    def test_019_get_order_total_price_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Dish import Dish
        from Business.Order import Order
        
        # Setup: Add dishes and orders
        dish1 = Dish(2000, "Total Price Dish 1", 15.50, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add dish 1')
        
        dish2 = Dish(2001, "Total Price Dish 2", 25.75, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add dish 2')
        
        dish3 = Dish(2002, "Total Price Dish 3", 10.25, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), 'add dish 3')
        
        # Order with no dishes, just delivery fee
        order1 = Order(2000, datetime.now(), 5.00, "200 Total Price Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add order 1')
        
        # Order with multiple dishes
        order2 = Order(2001, datetime.now(), 7.50, "201 Total Price Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add order 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2001, 2000, 2), 'add dish 1 to order 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2001, 2001, 1), 'add dish 2 to order 2')
        
        # Order with one dish with quantity 0
        order3 = Order(2002, datetime.now(), 3.50, "202 Total Price Boulevard")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), 'add order 3')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2002, 2002, 0), 'add dish 3 to order 3 with quantity 0')
        
        # Test order with no dishes (only delivery fee)
        self.assertEqual(5.00, Solution.get_order_total_price(2000), 'order with no dishes')
        
        # Test order with multiple dishes
        # Expected: (2 * 15.50) + (1 * 25.75) + 7.50 = 31.00 + 25.75 + 7.50 = 64.25
        self.assertEqual(64.25, Solution.get_order_total_price(2001), 'order with multiple dishes')
        
        # Test order with dish quantity 0
        # Expected: (0 * 10.25) + 3.50 = 3.50
        self.assertEqual(3.50, Solution.get_order_total_price(2002), 'order with zero quantity dish')
        
        # Test non-existent order
        self.assertEqual(0, Solution.get_order_total_price(9999), 'non-existent order')
        
        # Test order with negative ID
        self.assertEqual(0, Solution.get_order_total_price(-5), 'negative order ID')
        
        # Test after removing a dish from an order
        self.assertEqual(ReturnValue.OK, Solution.order_does_not_contain_dish(2001, 2001), 'remove dish 2 from order 2')
        # Expected after removal: (2 * 15.50) + 7.50 = 31.00 + 7.50 = 38.50
        self.assertEqual(38.50, Solution.get_order_total_price(2001), 'order after dish removal')
        
        # Test after deleting an order
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2002), 'delete order 3')
        self.assertEqual(0, Solution.get_order_total_price(2002), 'deleted order')

    def test_020_get_customers_spent_max_avg_amount_money_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Customer import Customer
        from Business.Dish import Dish
        from Business.Order import Order
        
        # Setup: Add customers
        cust1 = Customer(2100, 'Max Avg Customer 1', 30, "1111111111")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add customer 1')
        
        cust2 = Customer(2101, 'Max Avg Customer 2', 35, "2222222222")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust2), 'add customer 2')
        
        cust3 = Customer(2102, 'Max Avg Customer 3', 40, "3333333333")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust3), 'add customer 3 (no orders)')
        
        # Setup: Add dishes
        dish1 = Dish(2100, "Max Avg Dish 1", 20.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add dish 1')
        
        dish2 = Dish(2101, "Max Avg Dish 2", 30.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add dish 2')
        
        # Setup: Add orders
        # Customer 1: Two orders
        order1 = Order(2100, datetime.now(), 5.00, "210 Max Avg Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add order 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2100, 2100), 'customer 1 places order 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2100, 2100, 1), 'add dish 1 to order 1')
        
        order2 = Order(2101, datetime.now(), 7.50, "211 Max Avg Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add order 2')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2100, 2101), 'customer 1 places order 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2101, 2101, 2), 'add dish 2 to order 2')
        # Order 2 total: 2 * 30.00 + 7.50 = 67.50
        # Customer 1 average: (25.00 + 67.50) / 2 = 46.25
        
        # Customer 2: One order with higher average
        order3 = Order(2102, datetime.now(), 10.00, "212 Max Avg Boulevard")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), 'add order 3')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2101, 2102), 'customer 2 places order 3')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2102, 2100, 1), 'add dish 1 to order 3')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2102, 2101, 1), 'add dish 2 to order 3')
        # Order 3 total: 20.00 + 30.00 + 10.00 = 60.00
        # Customer 2 average: 60.00 / 1 = 60.00
        
        # Test with one customer having highest average
        max_customers = Solution.get_customers_spent_max_avg_amount_money()
        self.assertEqual(1, len(max_customers), 'one customer with max average')
        self.assertEqual(2101, max_customers[0], 'customer 2 has highest average')
        
        # Test with multiple customers tied for highest average
        # Add another order for customer 1 to bring their average up to match customer 2
        order4 = Order(2103, datetime.now(), 8.50, "213 Max Avg Circle")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order4), 'add order 4')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2100, 2103), 'customer 1 places order 4')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2103, 2101, 2), 'add dish 2 to order 4')
        # Order 4 total: 2 * 30.00 + 8.50 = 68.50
        # Customer 1 new average: (25.00 + 67.50 + 68.50) / 3 = 161.00 / 3 = 53.67
        # Not quite enough to match customer 2's average of 60.00
        
        order5 = Order(2104, datetime.now(), 10.00, "214 Max Avg Lane")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order5), 'add order 5')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2100, 2104), 'customer 1 places order 5')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2104, 2101, 3), 'add dish 2 to order 5')
        # Order 5 total: 3 * 30.00 + 10.00 = 100.00
        # Customer 1 new average: (25.00 + 67.50 + 68.50 + 100.00) / 4 = 261.00 / 4 = 65.25
        # Now customer 1's average is higher than customer 2's
        
        max_customers_updated = Solution.get_customers_spent_max_avg_amount_money()
        self.assertEqual(1, len(max_customers_updated), 'one customer with new max average')
        self.assertEqual(2100, max_customers_updated[0], 'customer 1 now has highest average')
        
        # Make customer 2's average match customer 1's
        # Need to make customer 2's average = 65.25
        # Current total = 60.00, need new order to bring average to 65.25
        # 2 orders with total = 65.25 * 2 = 130.50
        # New order needs to be 130.50 - 60.00 = 70.50
        order6 = Order(2105, datetime.now(), 20.50, "215 Max Avg Drive")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order6), 'add order 6')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2101, 2105), 'customer 2 places order 6')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2105, 2101, 1), 'add dish 2 to order 6')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2105, 2100, 1), 'add dish 1 to order 6')
        # Order 6 total: 30.00 + 20.00 + 20.50 = 70.50
        # Customer 2 new average: (60.00 + 70.50) / 2 = 130.50 / 2 = 65.25
        # Now both customers have the same average
        
        max_customers_tied = Solution.get_customers_spent_max_avg_amount_money()
        self.assertEqual(2, len(max_customers_tied), 'two customers tied for max average')
        self.assertTrue(2100 in max_customers_tied and 2101 in max_customers_tied, 'both customers in result')
        # Check ascending order by ID
        self.assertEqual(2100, max_customers_tied[0], 'first customer ID')
        self.assertEqual(2101, max_customers_tied[1], 'second customer ID')
        
        # Test with no orders in the system (clear all orders first)
        # Need to delete all orders to clear the system
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2100), 'delete order 1')
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2101), 'delete order 2')
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2102), 'delete order 3')
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2103), 'delete order 4')
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2104), 'delete order 5')
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2105), 'delete order 6')
        
        empty_result = Solution.get_customers_spent_max_avg_amount_money()
        self.assertEqual(0, len(empty_result), 'no orders in system')
        
    def test_021_get_most_ordered_dish_in_period_edge_cases(self) -> None:
        from datetime import datetime, timedelta
        from Business.Dish import Dish, BadDish
        from Business.Order import Order
        
        # Setup: Create dates for time period testing
        now = datetime.now()
        period1_start = now - timedelta(days=30)  # 30 days ago
        period1_end = now - timedelta(days=20)    # 20 days ago
        
        period2_start = now - timedelta(days=19)  # 19 days ago
        period2_end = now - timedelta(days=10)    # 10 days ago
        
        period3_start = now - timedelta(days=9)   # 9 days ago
        period3_end = now                         # now
        
        # Setup: Add dishes
        dish1 = Dish(2200, "Period Dish 1", 15.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add dish 1')
        
        dish2 = Dish(2201, "Period Dish 2", 25.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add dish 2')
        
        dish3 = Dish(2202, "Period Dish 3", 10.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), 'add dish 3')
        
        # Setup: Add orders in different time periods
        # Period 1 orders
        order1 = Order(2200, period1_start + timedelta(days=2), 5.00, "220 Period Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add order 1 in period 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2200, 2200, 2), 'add dish 1 to order 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2200, 2201, 1), 'add dish 2 to order 1')
        
        order2 = Order(2201, period1_start + timedelta(days=5), 7.50, "221 Period Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add order 2 in period 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2201, 2200, 1), 'add dish 1 to order 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2201, 2202, 3), 'add dish 3 to order 2')
        # Period 1 counts: dish1=3, dish2=1, dish3=3
        
        # Period 2 orders
        order3 = Order(2202, period2_start + timedelta(days=2), 8.00, "222 Period Boulevard")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), 'add order 3 in period 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2202, 2201, 5), 'add dish 2 to order 3')
        
        order4 = Order(2203, period2_start + timedelta(days=7), 9.50, "223 Period Circle")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order4), 'add order 4 in period 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2203, 2201, 2), 'add dish 2 to order 4')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2203, 2202, 1), 'add dish 3 to order 4')
        # Period 2 counts: dish1=0, dish2=7, dish3=1
        
        # Period 3 orders
        order5 = Order(2204, period3_start + timedelta(days=2), 6.25, "224 Period Drive")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order5), 'add order 5 in period 3')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2204, 2200, 1), 'add dish 1 to order 5')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2204, 2202, 4), 'add dish 3 to order 5')
        
        order6 = Order(2205, period3_start + timedelta(days=5), 7.75, "225 Period Lane")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order6), 'add order 6 in period 3')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2205, 2202, 3), 'add dish 3 to order 6')
        # Period 3 counts: dish1=1, dish2=0, dish3=7
        
        # Test getting most ordered dish in period 1
        # In period 1, dish1 and dish3 are tied with 3 orders each, should return the one with smaller ID
        most_ordered_p1 = Solution.get_most_ordered_dish_in_period(period1_start, period1_end)
        self.assertEqual(2200, most_ordered_p1.get_dish_id(), 'most ordered dish in period 1')
        
        # Test getting most ordered dish in period 2
        # In period 2, dish2 has 7 orders, the most
        most_ordered_p2 = Solution.get_most_ordered_dish_in_period(period2_start, period2_end)
        self.assertEqual(2201, most_ordered_p2.get_dish_id(), 'most ordered dish in period 2')
        
        # Test getting most ordered dish in period 3
        # In period 3, dish3 has 7 orders, the most
        most_ordered_p3 = Solution.get_most_ordered_dish_in_period(period3_start, period3_end)
        self.assertEqual(2202, most_ordered_p3.get_dish_id(), 'most ordered dish in period 3')
        
        # Test across all periods
        # Across all periods: dish1=4, dish2=8, dish3=11
        most_ordered_all = Solution.get_most_ordered_dish_in_period(period1_start, period3_end)
        self.assertEqual(2202, most_ordered_all.get_dish_id(), 'most ordered dish across all periods')
        
        # Test with invalid period (end before start)
        invalid_period = Solution.get_most_ordered_dish_in_period(period1_end, period1_start)
        self.assertEqual(BadDish.__name__, invalid_period.__class__.__name__, 'invalid period')
        
        # Test with period containing no orders
        future_start = now + timedelta(days=1)
        future_end = now + timedelta(days=10)
        no_orders_period = Solution.get_most_ordered_dish_in_period(future_start, future_end)
        self.assertEqual(BadDish.__name__, no_orders_period.__class__.__name__, 'period with no orders')
        
        # Test with null parameters
        null_start = Solution.get_most_ordered_dish_in_period(None, period3_end)
        self.assertEqual(BadDish.__name__, null_start.__class__.__name__, 'null start date')
        
        null_end = Solution.get_most_ordered_dish_in_period(period1_start, None)
        self.assertEqual(BadDish.__name__, null_end.__class__.__name__, 'null end date')
        
        # Test after deleting dishes and orders
        # Delete dish 1 and then check period 1 again
        # Now in period 1, only dish3 has orders
        self.assertEqual(ReturnValue.OK, Solution.delete_order(2200), 'delete order with dish 1')
        most_ordered_p1_after_delete = Solution.get_most_ordered_dish_in_period(period1_start, period1_end)
        self.assertEqual(2202, most_ordered_p1_after_delete.get_dish_id(), 'most ordered dish in period 1 after delete')

    def test_022_did_customer_order_top_rated_dishes_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Customer import Customer
        from Business.Dish import Dish
        from Business.Order import Order
        
        # Setup: Add customers
        cust1 = Customer(2300, 'Top Rated Customer 1', 30, "9998887777")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add customer 1')
        
        cust2 = Customer(2301, 'Top Rated Customer 2', 35, "8887776666")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust2), 'add customer 2')
        
        cust3 = Customer(2302, 'Top Rated Customer 3', 40, "7776665555")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust3), 'add customer 3')
        
        cust4 = Customer(2303, 'Top Rated Customer 4', 45, "6665554444")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust4), 'add customer 4 (no orders)')
        
        # Setup: Add dishes
        dish1 = Dish(2300, "Top Rated Dish 1", 20.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add dish 1')
        
        dish2 = Dish(2301, "Top Rated Dish 2", 25.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add dish 2')
        
        dish3 = Dish(2302, "Top Rated Dish 3", 30.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), 'add dish 3')
        
        dish4 = Dish(2303, "Top Rated Dish 4", 35.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish4), 'add dish 4')
        
        dish5 = Dish(2304, "Top Rated Dish 5", 40.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish5), 'add dish 5')
        
        dish6 = Dish(2305, "Top Rated Dish 6", 45.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish6), 'add dish 6')
        
        # Setup: Add ratings to establish top 5 dishes
        # Dish 1: Average rating 5.0 (highest)
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2300, 2300, 5), 'customer 1 rates dish 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2301, 2300, 5), 'customer 2 rates dish 1')
        
        # Dish 2: Average rating 4.5
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2300, 2301, 5), 'customer 1 rates dish 2')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2301, 2301, 4), 'customer 2 rates dish 2')
        
        # Dish 3: Average rating 4.0
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2300, 2302, 4), 'customer 1 rates dish 3')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2301, 2302, 4), 'customer 2 rates dish 3')
        
        # Dish 4: Average rating 3.5
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2300, 2303, 4), 'customer 1 rates dish 4')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2301, 2303, 3), 'customer 2 rates dish 4')
        
        # Dish 5: Average rating 3.0
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2300, 2304, 3), 'customer 1 rates dish 5')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2301, 2304, 3), 'customer 2 rates dish 5')
        
        # Dish 6: Average rating 2.5 (not in top 5)
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2300, 2305, 3), 'customer 1 rates dish 6')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2301, 2305, 2), 'customer 2 rates dish 6')
        
        # Setup: Add orders for customers
        order1 = Order(2300, datetime.now(), 5.00, "230 Top Rated Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add order 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2300, 2300), 'customer 1 places order 1')
        # Customer 1 orders the top 3 dishes
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2300, 2300, 1), 'add dish 1 to order 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2300, 2301, 1), 'add dish 2 to order 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2300, 2302, 1), 'add dish 3 to order 1')
        
        order2 = Order(2301, datetime.now(), 7.50, "231 Top Rated Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add order 2')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2301, 2301), 'customer 2 places order 2')
        # Customer 2 orders dishes 1, 4, and 6 (only dishes 1 and 4 are in top 5)
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2301, 2300, 1), 'add dish 1 to order 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2301, 2303, 1), 'add dish 4 to order 2')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2301, 2305, 1), 'add dish 6 to order 2')
        
        order3 = Order(2302, datetime.now(), 8.00, "232 Top Rated Boulevard")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), 'add order 3')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(2302, 2302), 'customer 3 places order 3')
        # Customer 3 orders only dish 6 (not in top 5)
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2302, 2305, 2), 'add dish 6 to order 3')
        
        # Test customer who ordered all 5 top-rated dishes
        # First, add the remaining top-rated dishes to customer 1's order
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2300, 2303, 1), 'add dish 4 to order 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2300, 2304, 1), 'add dish 5 to order 1')
        
        # Customer 1 should now have ordered all top 5 dishes
        self.assertEqual(True, Solution.did_customer_order_top_rated_dishes(2300), 'customer ordered all top 5')
        
        # Test customer who ordered some but not all top-rated dishes
        self.assertEqual(True, Solution.did_customer_order_top_rated_dishes(2301), 'customer ordered some top 5')
        
        # Test customer who ordered no top-rated dishes
        self.assertEqual(False, Solution.did_customer_order_top_rated_dishes(2302), 'customer ordered no top 5')
        
        # Test customer who placed no orders
        self.assertEqual(False, Solution.did_customer_order_top_rated_dishes(2303), 'customer placed no orders')
        
        # Test non-existent customer
        self.assertEqual(False, Solution.did_customer_order_top_rated_dishes(9999), 'non-existent customer')
        
        # Test customer with negative ID
        self.assertEqual(False, Solution.did_customer_order_top_rated_dishes(-5), 'negative customer ID')
        
        # Test after changing ratings to change the top 5
        # Delete ratings first, then add high ratings to make dish 6 have higher rating than dish 5
        self.assertEqual(ReturnValue.OK, Solution.customer_deleted_rating_on_dish(2300, 2305), 'delete customer 1 rating for dish 6')
        self.assertEqual(ReturnValue.OK, Solution.customer_deleted_rating_on_dish(2301, 2305), 'delete customer 2 rating for dish 6')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2300, 2305, 5), 'add high rating from customer 1 for dish 6')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(2301, 2305, 5), 'add high rating from customer 2 for dish 6')
        # Now dish 6 should be in top 5 and dish 5 should not
        
        # Customer 2 now has ordered 2 top-rated dishes (1 and 6), but not all 5
        self.assertEqual(True, Solution.did_customer_order_top_rated_dishes(2301), 'customer 2 after ratings change')
        
        # Customer 3 now has ordered 1 top-rated dish (6), but not all 5
        self.assertEqual(True, Solution.did_customer_order_top_rated_dishes(2302), 'customer 3 after ratings change')
        
        # Add dish 6 to customer 1's order so they still have all top 5
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(2300, 2305, 1), 'add dish 6 to order 1')
        self.assertEqual(True, Solution.did_customer_order_top_rated_dishes(2300), 'customer 1 still has all top 5')

    def test_023_get_customers_rated_but_not_ordered_edge_cases(self) -> None:
        from datetime import datetime
        from Business.Customer import Customer
        from Business.Dish import Dish
        from Business.Order import Order
        
        empty_result = Solution.get_customers_rated_but_not_ordered()
        self.assertEqual(0, len(empty_result), 'empty database')
        
        # Setup: Add customers
        cust1 = Customer(3000, 'Rate No Order Customer 1', 30, "1111222233")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust1), 'add customer 1')
        
        cust2 = Customer(3001, 'Rate No Order Customer 2', 35, "2222333344")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust2), 'add customer 2')
        
        cust3 = Customer(3002, 'Rate No Order Customer 3', 40, "3333444455")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust3), 'add customer 3')
        
        cust4 = Customer(3003, 'Rate No Order Customer 4', 45, "4444555566")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(cust4), 'add customer 4')
        
        # Setup: Add dishes
        dish1 = Dish(3000, "Rate No Order Dish 1", 20.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), 'add dish 1')
        
        dish2 = Dish(3001, "Rate No Order Dish 2", 25.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), 'add dish 2')
        
        dish3 = Dish(3002, "Rate No Order Dish 3", 30.00, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), 'add dish 3')
        
        # Setup: Add orders and connect to customers
        order1 = Order(3000, datetime.now(), 5.00, "300 Rate No Order Street")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), 'add order 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(3000, 3000), 'customer 1 places order 1')
        
        order2 = Order(3001, datetime.now(), 7.50, "301 Rate No Order Avenue")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), 'add order 2')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(3001, 3001), 'customer 2 places order 2')
        
        # Setup: Add dishes to orders
        # Customer 1 orders dishes 1 and 2
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(3000, 3000, 1), 'add dish 1 to order 1')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(3000, 3001, 2), 'add dish 2 to order 1')
        
        # Customer 2 orders dish 2
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(3001, 3001, 1), 'add dish 2 to order 2')
        
        # Setup: Add ratings
        # Customer 1 rates dishes 1, 2 and 3 (but only ordered 1 and 2)
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(3000, 3000, 5), 'customer 1 rates dish 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(3000, 3001, 4), 'customer 1 rates dish 2')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(3000, 3002, 3), 'customer 1 rates dish 3')
        
        # Customer 2 rates dishes 1 and 2 (but only ordered 2)
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(3001, 3000, 4), 'customer 2 rates dish 1')
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(3001, 3001, 5), 'customer 2 rates dish 2')
        
        # Customer 3 rates dish 1 (but ordered nothing)
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(3002, 3000, 3), 'customer 3 rates dish 1')
        
        # Customer 4 rates nothing and orders nothing
        
        # Test initial state
        # Expected result: Customers 1, 2, and 3 have rated dishes they didn't order
        rated_not_ordered = Solution.get_customers_rated_but_not_ordered()
        self.assertEqual(3, len(rated_not_ordered), 'three customers rated but not ordered')
        self.assertIn(3000, rated_not_ordered, 'customer 1 in result')
        self.assertIn(3001, rated_not_ordered, 'customer 2 in result')
        self.assertIn(3002, rated_not_ordered, 'customer 3 in result')
        
        # Test after customer 1 orders dish 3 (which they previously only rated)
        order3 = Order(3002, datetime.now(), 6.25, "302 Rate No Order Boulevard")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), 'add order 3')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(3000, 3002), 'customer 1 places order 3')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(3002, 3002, 1), 'add dish 3 to order 3')
        
        rated_not_ordered_updated = Solution.get_customers_rated_but_not_ordered()
        self.assertEqual(2, len(rated_not_ordered_updated), 'two customers rated but not ordered')
        self.assertNotIn(3000, rated_not_ordered_updated, 'customer 1 not in result anymore')
        self.assertIn(3001, rated_not_ordered_updated, 'customer 2 still in result')
        self.assertIn(3002, rated_not_ordered_updated, 'customer 3 still in result')
        
        # Test after customer 2 orders dish 1 (which they previously only rated)
        order4 = Order(3003, datetime.now(), 8.75, "303 Rate No Order Drive")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order4), 'add order 4')
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(3001, 3003), 'customer 2 places order 4')
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(3003, 3000, 1), 'add dish 1 to order 4')
        
        rated_not_ordered_updated2 = Solution.get_customers_rated_but_not_ordered()
        self.assertEqual(1, len(rated_not_ordered_updated2), 'one customer rated but not ordered')
        self.assertNotIn(3000, rated_not_ordered_updated2, 'customer 1 not in result')
        self.assertNotIn(3001, rated_not_ordered_updated2, 'customer 2 not in result anymore')
        self.assertIn(3002, rated_not_ordered_updated2, 'customer 3 still in result')
        
        # Test after removing all ratings for customer 3
        self.assertEqual(ReturnValue.OK, Solution.customer_deleted_rating_on_dish(3002, 3000), 'delete customer 3 rating')
        
        rated_not_ordered_updated3 = Solution.get_customers_rated_but_not_ordered()
        self.assertEqual(0, len(rated_not_ordered_updated3), 'no customers rated but not ordered')
        
    
    def test_024_get_non_worth_price_increase_edge_cases(self) -> None:
        from Business.Dish import Dish
        from Business.Order import Order
        from datetime import datetime
        
        
        # Test with no dishes in the database
        empty_result = Solution.get_non_worth_price_increase()
        self.assertEqual(empty_result, [], "Expected empty list when no dishes exist")
        
        # Test with a dish that should NOT be worth price increase (sells better at lower price)
        dish1 = Dish(4500, "Pizza Margherita", 50.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), "Add dish 1")
        
        # Create orders with dish1 at current price (50.0)
        order1 = Order(4500, datetime(2023, 1, 1, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), "Add order 1")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4500, 4500, 2), "Add 2 pizzas at current price")
        
        # Update dish price to a lower value
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4500, 40.0), "Update dish price to lower value")
        
        # Create orders with dish1 at lower price (40.0)
        order2 = Order(4501, datetime(2023, 1, 2, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), "Add order 2")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4501, 4500, 5), "Add 5 pizzas at lower price")
        
        # Set price back to original for testing
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4500, 50.0), "Reset dish price to original")
        
        # Test with a dish that should be worth price increase (sells better at higher price)
        dish2 = Dish(4501, "Premium Steak", 80.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), "Add dish 2")
        
        # Create orders with dish2 at current price (80.0)
        order3 = Order(4502, datetime(2023, 1, 3, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), "Add order 3")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4502, 4501, 2), "Add 2 steaks at current price")
        
        # Update dish price to a higher value
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4501, 100.0), "Update dish price to higher value")
        
        # Create orders with dish2 at higher price (100.0)
        order4 = Order(4503, datetime(2023, 1, 4, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order4), "Add order 4") 
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4503, 4501, 4), "Add 4 steaks at higher price")
        
        # Test with an inactive dish
        dish3 = Dish(4502, "Inactive Dish", 30.0, False)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), "Add inactive dish")
        
        # Need to set dish active to add it to orders
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(4502, True), "Set dish3 to active temporarily")
        
        # Create orders with dish3 at current price (30.0)
        order5 = Order(4504, datetime(2023, 1, 5, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order5), "Add order 5")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4504, 4502, 2), "Add 2 dishes at current price")
        
        # Update dish price to a lower value
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4502, 25.0), "Lower dish3 price")
        
        # Create orders with dish3 at lower price (25.0)
        order6 = Order(4505, datetime(2023, 1, 6, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order6), "Add order 6")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4505, 4502, 5), "Add 5 dishes at lower price")
        
        # Set dish3 back to original price and to inactive for testing
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4502, 30.0), "Reset dish3 price")
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(4502, False), "Set dish3 to inactive")
        
        # Test with a dish that has only been sold at one price point
        dish4 = Dish(4503, "Single Price Dish", 40.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish4), "Add dish with single price point")
        
        # Create orders with dish4 at its only price (40.0)
        order7 = Order(4506, datetime(2023, 1, 7, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order7), "Add order 7")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4506, 4503, 3), "Add 3 dishes at only price")
        
        # Test with a dish that has no orders
        dish5 = Dish(4504, "No Orders Dish", 35.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish5), "Add dish with no orders")
        
        # Test with a dish that has same sales at different prices
        dish6 = Dish(4505, "Same Sales Dish", 45.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish6), "Add dish with consistent sales")
        
        # Create orders with dish6 at current price (45.0)
        order8 = Order(4507, datetime(2023, 1, 8, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order8), "Add order 8")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4507, 4505, 3), "Add 3 dishes at current price")
        
        # Update dish price to a lower value
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4505, 40.0), "Update dish6 price to lower value")
        
        # Create orders with dish6 at lower price (40.0) with same average amount
        order9 = Order(4508, datetime(2023, 1, 9, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order9), "Add order 9")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4508, 4505, 3), "Add 3 dishes at lower price")
        
        # Set price back to original for testing
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4505, 45.0), "Reset dish6 price to original")
        
        # Test with a dish that has multiple price points (3+)
        dish7 = Dish(4506, "Multi Price Dish", 60.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish7), "Add dish with multiple price points")
        
        # Create orders at first price (60.0)
        order10 = Order(4509, datetime(2023, 1, 10), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order10), "Add order 10")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4509, 4506, 2), "Add 2 dishes at price 60.0")
        
        # Update price to second price point
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4506, 50.0), "Update dish7 to second price point")
        
        # Create orders at second price (50.0)
        order11 = Order(4510, datetime(2023, 1, 11), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order11), "Add order 11")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4510, 4506, 4), "Add 4 dishes at price 50.0")
        
        # Update price to third price point
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4506, 70.0), "Update dish7 to third price point")
        
        # Create orders at third price (70.0)
        order12 = Order(4511, datetime(2023, 1, 12), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order12), "Add order 12")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4511, 4506, 1), "Add 1 dish at price 70.0")
        
        # Set back to original price for testing
        self.assertEqual(ReturnValue.OK, Solution.update_dish_price(4506, 60.0), "Reset dish7 to original price")
        
        # Test results - should include dish1 and dish7 (ordered by ID)
        result = Solution.get_non_worth_price_increase()
        self.assertEqual(result, [4500, 4506], "Expected dishes that sell better at lower price in results")
        
    # Adding edge case tests for remaining Advanced API functions
    def test_025_get_cumulative_profit_per_month_edge_cases(self) -> None:
        from Business.Dish import Dish
        from Business.Customer import Customer
        from Business.Order import Order
        from datetime import datetime
        
        # Test with no orders in the database
        empty_result = Solution.get_cumulative_profit_per_month(2023)
        self.assertEqual(empty_result, [], "Expected empty list when no orders exist")
        
        # Setup data for testing
        customer = Customer(4000, "Profit Test Customer", 25, "1234567890")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer), "Add customer for profit test")
        
        dish = Dish(4000, "Profit Test Pizza", 50.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish), "Add dish for profit test")
        
        # Test with orders in a single month
        # Create two orders in January 2023
        order1 = Order(4000, datetime(2023, 1, 10, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), "Add January order 1")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(4000, 4000), "Customer places January order 1")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4000, 4000, 2), "Add 2 dishes to January order 1")
        
        order2 = Order(4001, datetime(2023, 1, 20, 18, 0, 0), 15.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), "Add January order 2")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(4000, 4001), "Customer places January order 2")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4001, 4000, 3), "Add 3 dishes to January order 2")
        
        # Total profit for January: (2*50)+10 + (3*50)+15 = $110 + $165 = $275
        january_result = Solution.get_cumulative_profit_per_month(2023)
        self.assertEqual(january_result, [(1, 275.0)], "Expected cumulative profit of $275 for January")
        
        # Test with orders spread across multiple months
        # February order
        order3 = Order(4002, datetime(2023, 2, 15, 18, 0, 0), 15.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order3), "Add February order")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(4000, 4002), "Customer places February order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4002, 4000, 1), "Add 1 dish to February order")
        
        # April order (skipping March)
        order4 = Order(4003, datetime(2023, 4, 5, 13, 0, 0), 20.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order4), "Add April order")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(4000, 4003), "Customer places April order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4003, 4000, 2), "Add 2 dishes to April order")
        
        # Check cumulative profit:
        # January: $275
        # February: $275 + (1*50)+15 = $340
        # March: No orders, not in result
        # April: $340 + (2*50)+20 = $460
        multi_month_result = Solution.get_cumulative_profit_per_month(2023)
        self.assertEqual(multi_month_result, [(1, 275.0), (2, 340.0), (4, 460.0)], 
                         "Expected correct cumulative profit across months with gaps")
        
        # Test with orders across different years
        # 2022 order
        order5 = Order(4004, datetime(2022, 12, 10, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order5), "Add 2022 order")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(4000, 4004), "Customer places 2022 order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4004, 4000, 2), "Add 2 dishes to 2022 order")
        
        # Check 2022 cumulative profit: (2*50)+10 = $110
        result_2022 = Solution.get_cumulative_profit_per_month(2022)
        self.assertEqual(result_2022, [(12, 110.0)], "Expected correct cumulative profit for 2022")
        
        # Check that 2023 profit is unaffected
        result_2023 = Solution.get_cumulative_profit_per_month(2023)
        self.assertEqual(result_2023, [(1, 275.0), (2, 340.0), (4, 460.0)], 
                         "Expected 2023 profit to be unaffected by 2022 orders")
        
        # Test with orders with multiple dish types
        # Add another dish
        dish2 = Dish(4001, "Profit Test Pasta", 40.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), "Add second dish for profit test")
        
        # June order with multiple dish types
        order6 = Order(4005, datetime(2023, 6, 10, 12, 0, 0), 10.0, "Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order6), "Add June order")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(4000, 4005), "Customer places June order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4005, 4000, 1), "Add 1 pizza to June order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(4005, 4001, 2), "Add 2 pastas to June order")
        
        # Check updated cumulative profit:
        # January: $275
        # February: $340
        # April: $460
        # June: $460 + (1*50)+(2*40)+10 = $600
        final_result = Solution.get_cumulative_profit_per_month(2023)
        self.assertEqual(final_result, [(1, 275.0), (2, 340.0), (4, 460.0), (6, 600.0)], 
                         "Expected correct final cumulative profit with multiple dish types")

    def test_026_get_potential_dish_recommendations_edge_cases(self) -> None:
        from Business.Dish import Dish
        from Business.Customer import Customer
        from Business.Order import Order
        from datetime import datetime
        
        # Test with non-existent customer
        nonexistent_result = Solution.get_potential_dish_recommendations(999)
        self.assertEqual(nonexistent_result, [], "Expected empty list for non-existent customer")
        
        # Setup customers for testing
        customer1 = Customer(5000, "Recommendation Test C1", 25, "1234567890")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer1), "Add customer 1 for recommendation test")
        
        customer2 = Customer(5001, "Recommendation Test C2", 30, "0987654321")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer2), "Add customer 2 for recommendation test")
        
        customer3 = Customer(5002, "Recommendation Test C3", 40, "1122334455")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer3), "Add customer 3 for recommendation test")
        
        # Test with a customer who has no similar customers
        no_similar_result = Solution.get_potential_dish_recommendations(5000)
        self.assertEqual(no_similar_result, [], "Expected empty list when no similar customers exist")
        
        # Create dishes for testing
        dish1 = Dish(5000, "Recommendation Pizza", 50.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), "Add dish 1 for recommendation test")
        
        dish2 = Dish(5001, "Recommendation Pasta", 40.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), "Add dish 2 for recommendation test")
        
        dish3 = Dish(5002, "Recommendation Dessert", 20.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), "Add dish 3 for recommendation test")
        
        # Setup for similarity testing by using the appropriate functions
        # First, clear any existing test data
        Solution.clear_tables()
        
        # Recreate our customers
        customer1 = Customer(5000, "Recommendation Test C1", 25, "1234567890")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer1), "Add customer 1 for recommendation test")
        
        customer2 = Customer(5001, "Recommendation Test C2", 30, "0987654321")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer2), "Add customer 2 for recommendation test")
        
        customer3 = Customer(5002, "Recommendation Test C3", 40, "1122334455")
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer3), "Add customer 3 for recommendation test")
        
        # Create dishes for testing
        dish1 = Dish(5000, "Recommendation Pizza", 50.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish1), "Add dish 1 for recommendation test")
        
        dish2 = Dish(5001, "Recommendation Pasta", 40.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish2), "Add dish 2 for recommendation test")
        
        dish3 = Dish(5002, "Recommendation Dessert", 20.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish3), "Add dish 3 for recommendation test")
        
        # Add several common dishes that both customers will rate identically to establish similarity
        # We need at least 3 identical ratings to establish similarity according to the view definition
        common_dish1 = Dish(5099, "Common Rated Dish 1", 30.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(common_dish1), "Add common dish 1 for ratings")
        common_dish2 = Dish(5098, "Common Rated Dish 2", 35.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(common_dish2), "Add common dish 2 for ratings")
        common_dish3 = Dish(5097, "Common Rated Dish 3", 40.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(common_dish3), "Add common dish 3 for ratings")
        
        # Both customers rate the common dishes identically to establish similarity
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5000, 5099, 5), "Customer 1 rates common dish 1")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5099, 5), "Customer 2 rates common dish 1")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5000, 5098, 4), "Customer 1 rates common dish 2")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5098, 4), "Customer 2 rates common dish 2")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5000, 5097, 3), "Customer 1 rates common dish 3")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5097, 3), "Customer 2 rates common dish 3")
        
        # Customer 1 ordered the dishes they rated
        order1 = Order(5000, datetime.now(), 10.0, "Recommendation Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), "Add order for customer 1")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(5000, 5000), "Customer 1 places order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(5000, 5099, 1), "Add dish 1 to customer 1's order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(5000, 5098, 2), "Add dish 2 to customer 1's order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(5000, 5097, 3), "Add dish 3 to customer 1's order")
        
        # Customer 2 rates dishes that will be recommendations
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5000, 5), 
                        "Similar customer rates dish 1 highly")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5001, 2), 
                        "Similar customer rates dish 2 poorly")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5002, 4), 
                        "Similar customer rates dish 3 highly")
        
        # Customer 1 should get recommendations for dishes 1 and 3 (rated > 3 by similar customer)
        no_orders_result = Solution.get_potential_dish_recommendations(5000)
        self.assertEqual(no_orders_result, [5000, 5002], 
                        "Expected recommendations for dishes with high ratings from similar customer")
        
        # Test the rating threshold
        # Make customer 3 similar to customer 1 by rating the same dishes with the same ratings
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5002, 5099, 5), 
                         "Customer 3 rates common dish 1")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5002, 5098, 4), 
                         "Customer 3 rates common dish 2")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5002, 5097, 3), 
                         "Customer 3 rates common dish 3")
        
        # Customer 3 rates dish 1 with rating = 3 (borderline)
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5002, 5000, 3),
                         "Customer 3 rates dish 1 with borderline rating")
        
        # Rating threshold is > 3, so this shouldn't change recommendations
        threshold_result = Solution.get_potential_dish_recommendations(5000)
        self.assertEqual(threshold_result, [5000, 5002],
                        "Expected same recommendations with borderline rating")
        
        # Test when customer already ordered recommended dishes
        # Customer 1 orders dish 1
        order1 = Order(5001, datetime.now(), 10.0, "Recommendation Address 123456")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order1), "Add order for customer 1")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(5000, 5001), "Customer 1 places order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(5001, 5000, 1), "Add dish 1 to customer 1's order")
        
        # Customer 1 should now only get recommendation for dish 3
        ordered_result = Solution.get_potential_dish_recommendations(5000)
        self.assertEqual(ordered_result, [5002], 
                         "Expected only dish 3 to be recommended after ordering dish 1")
        
        # Test with inactive dishes
        # Set dish 3 to inactive
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(5002, False), 
                         "Set dish 3 to inactive")
        
        # Inactive dishes should still be recommended if rated highly by similar customers
        inactive_result = Solution.get_potential_dish_recommendations(5000)
        self.assertEqual(inactive_result, [5002], 
                         "Expected inactive dish to still be recommended")
        
        # Set dish 3 back to active so we can order it
        self.assertEqual(ReturnValue.OK, Solution.update_dish_active_status(5002, True), 
                         "Set dish 3 back to active")
        
        # Test after ordering all recommended dishes
        order2 = Order(5002, datetime.now(), 15.0, "Recommendation Address 789012")
        self.assertEqual(ReturnValue.OK, Solution.add_order(order2), "Add second order for customer 1")
        self.assertEqual(ReturnValue.OK, Solution.customer_placed_order(5000, 5002), "Customer 1 places second order")
        self.assertEqual(ReturnValue.OK, Solution.order_contains_dish(5002, 5002, 1), "Add dish 3 to customer 1's order")
        
        # Customer 1 should now get no recommendations (ordered all highly rated dishes)
        no_recs_result = Solution.get_potential_dish_recommendations(5000)
        self.assertEqual(no_recs_result, [], 
                         "Expected no recommendations after ordering all recommended dishes")
                         
        # Test ordering of recommendations by dish_id
        # Add more dishes with non-sequential IDs
        dish4 = Dish(5007, "Recommendation Salad", 15.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish4), "Add dish 4 for recommendation test")
        
        dish5 = Dish(5003, "Recommendation Soup", 25.0, True)
        self.assertEqual(ReturnValue.OK, Solution.add_dish(dish5), "Add dish 5 for recommendation test")
        
        # Customer 2 rates the new dishes highly
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5003, 5), 
                         "Similar customer rates dish 5 highly")
        self.assertEqual(ReturnValue.OK, Solution.customer_rated_dish(5001, 5007, 4), 
                         "Similar customer rates dish 4 highly")
        
        # Customer 1 should get recommendations ordered by dish_id
        ordered_id_result = Solution.get_potential_dish_recommendations(5000)
        self.assertEqual(ordered_id_result, [5003, 5007], 
                         "Expected recommendations ordered by dish_id")

# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    # Install colorama if it's not already installed
    try:
        import colorama
        from colorama import init, Fore, Style
        init()  # Initialize colorama for Windows color support
        has_colors = True
    except ImportError:
        print("Installing colorama package for colored output...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "colorama"])
        from colorama import init, Fore, Style
        init()
        print(f"{Fore.GREEN}Colorama installed successfully!{Style.RESET_ALL}")
        has_colors = True
    
    # Create a custom test runner class inline
    class BeautifulTestResult(unittest.TextTestResult):
        def __init__(self, stream, descriptions, verbosity):
            super().__init__(stream, descriptions, verbosity)
            self.start_times = {}
            
        def startTest(self, test):
            self.start_times[test] = time.time()
            super().startTest(test)
            
        def addSuccess(self, test):
            duration = time.time() - self.start_times.get(test, time.time())
            test_name = str(test).split(' ')[0]
            self.stream.write(f"\r{test_name:<60} {Fore.GREEN}OK{Style.RESET_ALL} {duration:.4f}s\n")
            super().addSuccess(test)
            
        def addError(self, test, err):
            duration = time.time() - self.start_times.get(test, time.time())
            test_name = str(test).split(' ')[0]
            self.stream.write(f"\r{test_name:<60} {Fore.MAGENTA}ERROR{Style.RESET_ALL} {duration:.4f}s\n")
            super().addError(test, err)
            
        def addFailure(self, test, err):
            duration = time.time() - self.start_times.get(test, time.time())
            test_name = str(test).split(' ')[0]
            self.stream.write(f"\r{test_name:<60} {Fore.RED}FAIL{Style.RESET_ALL} {duration:.4f}s\n")
            super().addFailure(test, err)
            
        def addSkip(self, test, reason):
            duration = time.time() - self.start_times.get(test, time.time())
            test_name = str(test).split(' ')[0]
            self.stream.write(f"\r{test_name:<60} {Fore.YELLOW}SKIP{Style.RESET_ALL} {duration:.4f}s\n")
            super().addSkip(test, reason)
            
    class BeautifulTestRunner(unittest.TextTestRunner):
        def __init__(self, *args, **kwargs):
            kwargs['resultclass'] = BeautifulTestResult
            super().__init__(*args, **kwargs)
            
        def run(self, test):
            result = super().run(test)
            self._print_summary(result)
            return result
            
        def _print_summary(self, result):
            self.stream.write("\n" + "="*80 + "\n")
            self.stream.write(f"{Fore.CYAN}TEST RESULTS SUMMARY{Style.RESET_ALL}\n")
            self.stream.write("-"*80 + "\n")
            
            total = result.testsRun
            failures = len(result.failures)
            errors = len(result.errors)
            skipped = len(result.skipped)
            passed = total - failures - errors - skipped
            
            self.stream.write(f"Total: {total} | ")
            self.stream.write(f"{Fore.GREEN}Passed: {passed}{Style.RESET_ALL} | ")
            
            if failures > 0:
                self.stream.write(f"{Fore.RED}Failed: {failures}{Style.RESET_ALL} | ")
            else:
                self.stream.write(f"Failed: {failures} | ")
                
            if errors > 0:
                self.stream.write(f"{Fore.MAGENTA}Errors: {errors}{Style.RESET_ALL} | ")
            else:
                self.stream.write(f"Errors: {errors} | ")
                
            if skipped > 0:
                self.stream.write(f"{Fore.YELLOW}Skipped: {skipped}{Style.RESET_ALL}")
            else:
                self.stream.write(f"Skipped: {skipped}")
                
            # Print overall result
            self.stream.write("\n")
            if failures == 0 and errors == 0:
                self.stream.write(f"{Fore.GREEN}✓ All tests passed!{Style.RESET_ALL}\n")
            else:
                self.stream.write(f"{Fore.RED}✗ Some tests failed or had errors.{Style.RESET_ALL}\n")
            
            self.stream.write("="*80 + "\n")
    
    # Run with our beautiful test runner
    import time
    runner = BeautifulTestRunner(verbosity=1)
    runner.run(unittest.TestLoader().loadTestsFromTestCase(Test))
