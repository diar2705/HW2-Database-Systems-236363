# File: streamlit_app.py
import streamlit as st
import pandas as pd
from Solution import *
from Business.Customer import Customer
from Business.Dish import Dish
from Business.Order import Order
from Business.OrderDish import OrderDish
from Utility.ReturnValue import ReturnValue
import psycopg2
from datetime import datetime

DB_PARAMS = { #----------------TODO----------------
    "dbname": "cs236363",
    "user": "cs236363",
    "password": "cs236363",
    "host": "localhost",
    "port": 5432
}


def main():
    st.title("Yummify")

    if "db_initialized" not in st.session_state:
        st.session_state.db_initialized = False

    if st.button("Initialize Database (Drop/Create)"):
        drop_tables()
        create_tables()
        st.session_state.db_initialized = True
        st.success("Database initialized!")

    if not st.session_state.db_initialized:
        st.warning("Please initialize the database first.")
        return

    action = st.selectbox("Choose Action", [
        "Add Customer",
        "Add Dish",
        "Add Order",
        "Place Order",
        "Add Dish to Order",
        "Visualize Tables",
        "Total Price of Every Order",
        "Max Avg Spending",
        "Dishes ordered"
    ])

    if action == "Add Customer": 
        with st.form("Add Customer"):
            cust_id = st.number_input("Customer ID", min_value=1, step=1, format="%d")
            name = st.text_input("Full Name")
            age = st.number_input("Age", min_value=0, step=1, format="%d")
            phone = st.text_input("Phone (10 digits)")
            submitted = st.form_submit_button("Add Customer")
            if submitted:
                result = add_customer(Customer(cust_id, name, age, phone))
                if result == ReturnValue.OK:
                    st.success("Customer added successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Customer already exists.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input. Make sure all fields are valid (including a 10-digit phone number).")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Add Dish":
        with st.form("Add Dish"):
            dish_id = st.number_input("Dish ID", min_value=1, step=1, format="%d")
            name = st.text_input("Dish Name")
            price = st.number_input("Price", min_value=0.01, step=0.01, format="%.2f")
            is_active = st.checkbox("Is Active", value=True)
            submitted = st.form_submit_button("Add Dish")
            if submitted:
                from Business.Dish import Dish
                result = add_dish(Dish(dish_id, name, price, is_active))
                if result == ReturnValue.OK:
                    st.success("Dish added successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Dish already exists.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input. Make sure the dish name is at least 4 characters and price is positive.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Add Order":
        with st.form("Add Order"):
            order_id = st.number_input("Order ID", min_value=1, step=1, format="%d")
            date = st.date_input("Date")
            time = st.time_input("Time")
            delivery_fee = st.number_input("Delivery Fee", min_value=0.0, step=0.01, format="%.2f")
            delivery_address = st.text_input("Delivery Address")
            submitted = st.form_submit_button("Add Order")
            if submitted:
                from datetime import datetime
                from Business.Order import Order
                # Combine date and time into a datetime object
                date_time = datetime.combine(date, time)
                result = add_order(Order(order_id, date_time, delivery_fee, delivery_address))
                if result == ReturnValue.OK:
                    st.success("Order added successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Order already exists.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input. Make sure delivery address is at least 5 characters long and delivery fee is non-negative.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Place Order":
        with st.form("Place Order"):
            order_id = st.number_input("Order ID", min_value=1, step=1, format="%d")
            customer_id = st.number_input("Customer ID", min_value=1, step=1, format="%d")
            submitted = st.form_submit_button("Place Order")
            if submitted:
                result = customer_placed_order(customer_id, order_id)
                if result == ReturnValue.OK:
                    st.success("Order placed successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("This customer has already placed this order.")
                elif result == ReturnValue.NOT_EXISTS:
                    st.error("Customer or order does not exist.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Add Dish to Order":
        with st.form("Add Dish to Order"):
            order_id = st.number_input("Order ID", min_value=1, step=1, format="%d")
            dish_id = st.number_input("Dish ID", min_value=1, step=1, format="%d")
            amount = st.number_input("Amount", min_value=1, step=1, format="%d")
            submitted = st.form_submit_button("Add Dish to Order")
            if submitted:
                result = order_contains_dish(order_id, dish_id, amount)
                if result == ReturnValue.OK:
                    st.success("Dish added to order successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("This dish is already in the order.")
                elif result == ReturnValue.NOT_EXISTS:
                    st.error("Order or dish does not exist, or dish is not active.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid amount.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Visualize Tables":
        st.subheader("Customers")
        res = Connector.DBConnector().execute("SELECT * FROM customers")[1]
        st.dataframe(pd.DataFrame(res.rows))

        st.subheader("Orders")
        res = Connector.DBConnector().execute("SELECT * FROM orders")[1]
        st.dataframe(pd.DataFrame(res.rows))

        st.subheader("Dishes")
        res = Connector.DBConnector().execute("SELECT * FROM dishes")[1]
        st.dataframe(pd.DataFrame(res.rows))
        


    elif action == "Total Price of Every Order": 
        st.subheader("Total Price of Every Order")
        # Use the total_price_per_order view
        res = Connector.DBConnector().execute("SELECT order_id, total_price FROM total_price_per_order ORDER BY order_id")[1]
        if not res.isEmpty():
            st.dataframe(pd.DataFrame(res.rows))
        else:
            st.info("No orders found.")

    elif action == "Max Avg Spending":
        st.subheader("Customers with Maximum Average Spending")
        max_spending_customers = get_customers_spent_max_avg_amount_money()
        if max_spending_customers:
            # Convert to DataFrame for display
            df = pd.DataFrame({"Customer ID": max_spending_customers})
            st.dataframe(df)
            
            # Display additional information about these customers
            st.subheader("Customer Details")
            customer_details = []
            for cust_id in max_spending_customers:
                customer = get_customer(cust_id)
                customer_details.append({
                    "Customer ID": customer.get_cust_id(),
                    "Name": customer.get_full_name(),
                    "Age": customer.get_age(),
                    "Phone": customer.get_phone()
                })
            if customer_details:
                st.dataframe(pd.DataFrame(customer_details))
        else:
            st.info("No customers found with orders.")

    elif action == "Dishes ordered":
        order_id = st.number_input("Enter Order ID", min_value=1, step=1, format="%d")
        if st.button("Show Dishes"):
            order_dishes = get_all_order_items(order_id)
            if order_dishes:
                # Convert to DataFrame for display
                dishes_data = []
                for order_dish in order_dishes:
                    dish = get_dish(order_dish.get_dish_id())
                    dishes_data.append({
                        "Dish ID": order_dish.get_dish_id(),
                        "Dish Name": dish.get_name(),
                        "Amount": order_dish.get_amount(),
                        "Unit Price": order_dish.get_price() / order_dish.get_amount(),
                        "Total Price": order_dish.get_price()
                    })
                
                st.dataframe(pd.DataFrame(dishes_data))
                
                # Display order total
                total_price = get_order_total_price(order_id)
                st.subheader(f"Order Total: ${total_price:.2f}")
                
                # Display customer info
                customer = get_customer_that_placed_order(order_id)
                if customer.get_cust_id() is not None:
                    st.subheader("Customer Information")
                    st.write(f"Customer ID: {customer.get_cust_id()}")
                    st.write(f"Name: {customer.get_full_name()}")
                    st.write(f"Phone: {customer.get_phone()}")
                    st.write(f"Age: {customer.get_age()}")
            else:
                st.info(f"No dishes found for Order #{order_id} or order does not exist.")


if __name__ == "__main__":
    main()