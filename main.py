import pandas as pd
import numpy as np
import random
import time
from faker import Faker
import json
import psycopg2

# Connect to ExCompany database on PostgreSQL

db_connection = psycopg2.connect(
    dbname="ExCompany",
    user="postgres",
    password="<password>",
    host="localhost",
    port=5432,
)

# Create a cursor object to execute SQL queries
cursor = db_connection.cursor()

# Create a Faker instance
fake = Faker()

# Define area code for company phone numbers
company_area_code = 312

# Define the bounds for latitude and longitude of Chicago (where employees live)
chicago_latitude_bounds = (41.6445, 42.023)
chicago_longitude_bounds = (-87.9401, -87.524)


# Generate random data for each column
def create_faker_data(num_rows):
    employee_data = []
    for _ in range(0, num_rows):
        employee_id = fake.unique.random_int(
            min=100000000, max=999999999
        )  # Random 9-digit employee ID
        first_name = fake.first_name()
        last_name = fake.last_name()
        age = int(
            np.clip(np.random.normal(loc=35, scale=10), 18, 65)
        )  # Random age between 18 and 65 drawn from normal distribution with mean = 35, sd = 10
        rating = max(
            0, min(5, round(np.random.normal(loc=3, scale=1), 2))
        )  # Random rating between 0 and 5 drawn from normal distribution with mean = 3, sd = 1

        # Contact info for JSON and BJSON columns
        email = f"{first_name.lower()}.{last_name.lower()}@company.com"  # Generate email based on name
        phone_number = f"{company_area_code}-555-{fake.random_int(min = 1000, max = 9999)}"  # Generate phone number with fixed area code and random extension number

        # Create JSON and BJSON contact info
        json_contact_info = {
            "phone": phone_number,
            "email": email,
        }
        bjson_contact_info = json.dumps(
            json_contact_info
        )  # Serialize BJSON data to string using json.dumps()

        # Info for address geometry column (drawn from uniform distribution)
        latitude = random.uniform(
            chicago_latitude_bounds[0], chicago_latitude_bounds[1]
        )
        longitude = random.uniform(
            chicago_longitude_bounds[0], chicago_longitude_bounds[1]
        )
        address = f"POINT({longitude} {latitude})"  # Random point within Chicago bounds

        employee_data.append(
            (
                employee_id,
                first_name,
                last_name,
                age,
                rating,
                json.dumps(json_contact_info),
                json.dumps(bjson_contact_info),
                address,
            )
        )
    return employee_data


def create_full_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    # Row by row insertion using python and psycopg2 of the whole dataset
    full_query_times = []
    for i in range(0, reps):
        employee_data = create_faker_data(faker_entries)
        insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(insert_query_full, row)
        toc = time.perf_counter()
        full_query_times.append(toc - tic)
        cursor.execute("TRUNCATE TABLE employees")
    print(
        f"""Average full table insertion of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(full_query_times)/reps} seconds for a total of
        {sum(full_query_times)} seconds using Python and psycopg2
        """
    )
    return full_query_times


def create_text_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    # Row by row insertion using python and psycopg2 of
    # the text entries first name and last name with Employee ID
    text_query_times = []
    for i in range(0, reps):
        employee_data = create_faker_data(faker_entries)
        insert_query_text = """
            INSERT INTO employees 
            (employee_id, first_name, last_name)
            VALUES (%s, %s, %s)
            """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(insert_query_text, row[0:3])
        toc = time.perf_counter()
        text_query_times.append(toc - tic)
        cursor.execute("TRUNCATE TABLE employees")
    print(
        f"""Average text insertion of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(text_query_times)/reps} seconds for a total of
        {sum(text_query_times)} seconds using Python and psycopg2
        """
    )
    return text_query_times


def create_int_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    # Row by row insertion using python and psycopg2 of
    # the integer entry age with Employee ID
    int_query_times = []
    for i in range(0, reps):
        employee_data = create_faker_data(faker_entries)
        insert_query_int = """
            INSERT INTO employees
            (employee_id, age)
            VALUES (%s, %s)
            """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(insert_query_int, (row[0], row[3]))
        toc = time.perf_counter()
        int_query_times.append(toc - tic)
        cursor.execute("TRUNCATE TABLE employees")
    print(
        f"""Average integer insertion of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(int_query_times)/reps} seconds for a total of
        {sum(int_query_times)} seconds using Python and psycopg2
        """
    )
    return int_query_times


def create_float_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    # Row by row insertion using python and psycopg2 of
    # the float entry Rating and Employee ID
    float_query_times = []
    for i in range(0, reps):
        employee_data = create_faker_data(faker_entries)
        insert_query_float = """
            INSERT INTO employees
            (employee_id, rating)
            VALUES (%s, %s)
            """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(insert_query_float, (row[0], row[4]))
        toc = time.perf_counter()
        float_query_times.append(toc - tic)
        cursor.execute("TRUNCATE TABLE employees")
    print(
        f"""Average float insertion of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(float_query_times)/reps} seconds for a total of
        {sum(float_query_times)} seconds using Python and psycopg2
        """
    )
    return float_query_times


def create_json_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    # Row by row insertion using python and psycopg2 of
    # the JSON field json_contact_info and Employee ID
    json_query_times = []
    for i in range(0, reps):
        employee_data = create_faker_data(faker_entries)
        insert_query_json = """
            INSERT INTO employees
            (employee_id, json_contact_info)
            VALUES (%s, %s)
            """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(insert_query_json, (row[0], row[5]))
        toc = time.perf_counter()
        json_query_times.append(toc - tic)
        cursor.execute("TRUNCATE TABLE employees")
    print(
        f"""Average json insertion of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(json_query_times)/reps} seconds for a total of
        {sum(json_query_times)} seconds using Python and psycopg2
        """
    )
    return json_query_times


def create_bjson_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    # Row by row insertion using python and psycopg2 of
    # the BJSON field bjson_contact_info and Employee ID
    bjson_query_times = []
    for i in range(0, reps):
        employee_data = create_faker_data(faker_entries)
        insert_query_bjson = """
            INSERT INTO employees
            (employee_id, bjson_contact_info)
            VALUES (%s, %s)
            """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(insert_query_bjson, (row[0], row[6]))
        toc = time.perf_counter()
        bjson_query_times.append(toc - tic)
        cursor.execute("TRUNCATE TABLE employees")
    print(
        f"""Average bjson insertion of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(bjson_query_times)/reps} seconds for a total of
        {sum(bjson_query_times)} seconds using Python and psycopg2
        """
    )
    return bjson_query_times


def create_geometry_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    # Row by row insertion using python and psycopg2 of
    # the BJSON field bjson_contact_info and Employee ID
    geometry_query_times = []
    for i in range(0, reps):
        employee_data = create_faker_data(faker_entries)
        insert_query_geometry = """
            INSERT INTO employees
            (employee_id, address)
            VALUES (%s, %s)
            """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(insert_query_geometry, (row[0], row[7]))
        toc = time.perf_counter()
        geometry_query_times.append(toc - tic)
        cursor.execute("TRUNCATE TABLE employees")
    print(
        f"""Average geometry insertion of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(geometry_query_times)/reps} seconds for a total of
        {sum(geometry_query_times)} seconds using Python and psycopg2
        """
    )
    return geometry_query_times


def read_full_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    full_query_read_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        select_query_full = """
        SELECT employee_id, first_name, last_name, age,rating,
            json_contact_info, bjson_contact_info, address
        FROM employees 
        WHERE employee_id = %s AND first_name = %s AND last_name = %s
            AND age = %s AND rating = %s AND json_contact_info::jsonb = %s
            AND bjson_contact_info::jsonb = %s AND address::geometry = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(select_query_full, row)
        toc = time.perf_counter()
        full_query_read_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average full query read time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(full_query_read_times)/reps} seconds for a total of
        {sum(full_query_read_times)} seconds using Python and psycopg2
        """)
    return full_query_read_times


def update_full_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    full_query_update_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    initial_samples = random.sample(employee_data, num_rows)
    for i in range(0, reps):
        update_query_full = """
        UPDATE employees
        SET first_name = %s, last_name = %s, age = %s, rating = %s,
            json_contact_info = %s, bjson_contact_info = %s,
            address = %s
        WHERE employee_id = %s AND first_name = %s AND last_name = %s
            AND age = %s AND rating = %s AND json_contact_info::jsonb = %s
            AND bjson_contact_info::jsonb = %s and address::geometry = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(update_query_full, initial_samples[j][1:] + sampled_data[j])
        toc = time.perf_counter()
        full_query_update_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average full query update time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(full_query_update_times)/reps} seconds for a total of
        {sum(full_query_update_times)} seconds using Python and psycopg2
        """)
    return full_query_update_times


def delete_full_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    full_query_delete_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        delete_query_full = """
        DELETE FROM employees
        WHERE employee_id = %s AND first_name = %s AND last_name = %s
            AND age = %s AND rating = %s AND json_contact_info::jsonb = %s
            AND bjson_contact_info::jsonb = %s and address::geometry = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(delete_query_full, sampled_data[j])
            db_connection.rollback()
        toc = time.perf_counter()
        full_query_delete_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average full query delete time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(full_query_delete_times)/reps} seconds for a total of
        {sum(full_query_delete_times)} seconds using Python and psycopg2
        """)
    return full_query_delete_times


def read_text_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    text_query_read_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        select_query_text = """
        SELECT employee_id, address
        FROM employees
        WHERE employee_id = %s AND first_name = %s
            AND last_name = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(select_query_text, row[0:3])
        toc = time.perf_counter()
        text_query_read_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average text query read time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(text_query_read_times)/reps} seconds for a total of
        {sum(text_query_read_times)} seconds using Python and psycopg2
        """)
    return text_query_read_times


def update_text_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    text_query_update_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    initial_samples = random.sample(employee_data, num_rows)
    for i in range(0, reps):
        update_query_text = """
        UPDATE employees
        SET first_name = %s, last_name = %s
        WHERE employee_id = %s AND first_name = %s
            AND last_name = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                update_query_text,
                (initial_samples[j][1:3] + sampled_data[j][0:3]),
            )
        toc = time.perf_counter()
        text_query_update_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average text query update time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(text_query_update_times)/reps} seconds for a total of
        {sum(text_query_update_times)} seconds using Python and psycopg2
        """)
    return text_query_update_times


def delete_text_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    text_query_delete_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        delete_query_text = """
        DELETE FROM employees
        WHERE employee_id = %s AND first_name = %s
            AND last_name = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(delete_query_text, sampled_data[j][0:3])
            db_connection.rollback()
        toc = time.perf_counter()
        text_query_delete_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average text query delete time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(text_query_delete_times)/reps} seconds for a total of
        {sum(text_query_delete_times)} seconds using Python and psycopg2
        """)
    return text_query_delete_times


def read_integer_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    integer_query_read_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        select_query_integer = """
        SELECT employee_id, age
        FROM employees
        WHERE employee_id = %s AND age = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(select_query_integer, (row[0], row[3]))
        toc = time.perf_counter()
        integer_query_read_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average integer query read time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(integer_query_read_times)/reps} seconds for a total of
        {sum(integer_query_read_times)} seconds using Python and psycopg2
        """)
    return integer_query_read_times


def update_integer_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    integer_query_update_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    initial_samples = random.sample(employee_data, num_rows)
    for i in range(0, reps):
        update_query_integer = """
        UPDATE employees
        SET age = %s
        WHERE employee_id = %s AND age = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                update_query_integer,
                (initial_samples[j][3],) + (sampled_data[j][0], sampled_data[j][3]),
            )
        toc = time.perf_counter()
        integer_query_update_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average integer query update time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(integer_query_update_times)/reps} seconds for a total of
        {sum(integer_query_update_times)} seconds using Python and psycopg2
        """)
    return integer_query_update_times


def delete_integer_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    integer_query_delete_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        delete_query_integer = """
        DELETE FROM employees
        WHERE employee_id = %s AND age = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                delete_query_integer, (sampled_data[j][0], sampled_data[j][3])
            )
            db_connection.rollback()
        toc = time.perf_counter()
        integer_query_delete_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average integer query delete time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(integer_query_delete_times)/reps} seconds for a total of
        {sum(integer_query_delete_times)} seconds using Python and psycopg2
        """)
    return integer_query_delete_times


def read_float_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    float_query_read_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        select_query_float = """
        SELECT employee_id, rating
        FROM employees
        WHERE employee_id = %s AND rating = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(select_query_float, (row[0], row[4]))
        toc = time.perf_counter()
        float_query_read_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average float query read time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(float_query_read_times)/reps} seconds for a total of
        {sum(float_query_read_times)} seconds using Python and psycopg2
        """)
    return float_query_read_times


def update_float_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    float_query_update_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    initial_samples = random.sample(employee_data, num_rows)
    for i in range(0, reps):
        update_query_float = """
        UPDATE employees
        SET rating = %s
        WHERE employee_id = %s AND rating = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                update_query_float,
                (initial_samples[j][4],) + (sampled_data[j][0], sampled_data[j][4]),
            )
        toc = time.perf_counter()
        float_query_update_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average float query update time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(float_query_update_times)/reps} seconds for a total of
        {sum(float_query_update_times)} seconds using Python and psycopg2
        """)
    return float_query_update_times


def delete_float_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    float_query_delete_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        delete_query_float = """
        DELETE FROM employees
        WHERE employee_id = %s AND rating = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(delete_query_float, (sampled_data[j][0], sampled_data[j][4]))
            db_connection.rollback()
        toc = time.perf_counter()
        float_query_delete_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average float query delete time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(float_query_delete_times)/reps} seconds for a total of
        {sum(float_query_delete_times)} seconds using Python and psycopg2
        """)
    return float_query_delete_times


def read_json_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    json_query_read_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        select_query_json = """
        SELECT employee_id, json_contact_info
        FROM employees
        WHERE employee_id = %s AND json_contact_info::jsonb = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(select_query_json, (row[0], row[5]))
        toc = time.perf_counter()
        json_query_read_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average json query read time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(json_query_read_times)/reps} seconds for a total of
        {sum(json_query_read_times)} seconds using Python and psycopg2
        """)
    return json_query_read_times


def update_json_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    json_query_update_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    initial_samples = random.sample(employee_data, num_rows)
    for i in range(0, reps):
        update_query_json = """
        UPDATE employees
        SET json_contact_info = %s
        WHERE employee_id = %s AND json_contact_info::jsonb = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                update_query_json,
                (initial_samples[j][5],) + (sampled_data[j][0], sampled_data[j][5]),
            )
        toc = time.perf_counter()
        json_query_update_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average json query update time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(json_query_update_times)/reps} seconds for a total of
        {sum(json_query_update_times)} seconds using Python and psycopg2
        """)
    return json_query_update_times


def delete_json_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    json_query_delete_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        delete_query_json = """
        DELETE FROM employees
        WHERE employee_id = %s AND json_contact_info::jsonb = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(delete_query_json, (sampled_data[j][0], sampled_data[j][5]))
            db_connection.rollback()
        toc = time.perf_counter()
        json_query_delete_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average json query delete time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(json_query_delete_times)/reps} seconds for a total of
        {sum(json_query_delete_times)} seconds using Python and psycopg2
        """)
    return json_query_delete_times


def read_bjson_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    bjson_query_read_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        select_query_bjson = """
        SELECT employee_id, bjson_contact_info
        FROM employees
        WHERE employee_id = %s AND bjson_contact_info::jsonb = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(select_query_bjson, (row[0], row[6]))
        toc = time.perf_counter()
        bjson_query_read_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average bjson query read time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(bjson_query_read_times)/reps} seconds for a total of
        {sum(bjson_query_read_times)} seconds using Python and psycopg2
        """)
    return bjson_query_read_times


def update_bjson_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    bjson_query_update_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    initial_samples = random.sample(employee_data, num_rows)
    for i in range(0, reps):
        update_query_bjson = """
        UPDATE employees
        SET bjson_contact_info = %s
        WHERE employee_id = %s AND bjson_contact_info::jsonb = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                update_query_bjson,
                (initial_samples[j][6],) + (sampled_data[j][0], sampled_data[j][6]),
            )
        toc = time.perf_counter()
        bjson_query_update_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average bjson query update time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(bjson_query_update_times)/reps} seconds for a total of
        {sum(bjson_query_update_times)} seconds using Python and psycopg2
        """)
    return bjson_query_update_times


def delete_bjson_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    bjson_query_delete_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        delete_query_json = """
        DELETE FROM employees
        WHERE employee_id = %s AND bjson_contact_info::jsonb = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(delete_query_json, (sampled_data[j][0], sampled_data[j][6]))
            db_connection.rollback()
        toc = time.perf_counter()
        bjson_query_delete_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average bjson query delete time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(bjson_query_delete_times)/reps} seconds for a total of
        {sum(bjson_query_delete_times)} seconds using Python and psycopg2
        """)
    return bjson_query_delete_times


def read_geometry_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    geometry_query_read_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        select_query_geometry = """
        SELECT employee_id, address
        FROM employees
        WHERE employee_id = %s AND address::geometry = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for row in sampled_data:
            cursor.execute(select_query_geometry, (row[0], row[7]))
        toc = time.perf_counter()
        geometry_query_read_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average geometry query read time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(geometry_query_read_times)/reps} seconds for a total of
        {sum(geometry_query_read_times)} seconds using Python and psycopg2
        """)
    return geometry_query_read_times


def update_geometry_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    geometry_query_update_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    initial_samples = random.sample(employee_data, num_rows)
    for i in range(0, reps):
        update_query_geometry = """
        UPDATE employees
        SET address = %s
        WHERE employee_id = %s AND address::geometry = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                update_query_geometry,
                (initial_samples[j][7],) + (sampled_data[j][0], sampled_data[j][7]),
            )
        toc = time.perf_counter()
        geometry_query_update_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average geometry query update time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(geometry_query_update_times)/reps} seconds for a total of
        {sum(geometry_query_update_times)} seconds using Python and psycopg2
        """)
    return geometry_query_update_times


def delete_geometry_query(reps, num_rows, faker_entries):
    """
    reps {int}: number of repetitions of simulating the insertion
    num_rows {int}: number of rows to add to the employees table
    faker_entries {int}: number of fake entries to sample from without replacement
    """
    geometry_query_delete_times = []
    employee_data = create_faker_data(faker_entries)
    insert_query_full = """
            INSERT INTO employees 
            (employee_id, first_name, last_name, age, rating,
            json_contact_info, bjson_contact_info, address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    for row in employee_data:
        cursor.execute(insert_query_full, row)
    for i in range(0, reps):
        delete_query_geometry = """
        DELETE FROM employees
        WHERE employee_id = %s AND address::geometry = %s
        """
        sampled_data = random.sample(employee_data, num_rows)
        tic = time.perf_counter()
        for j in range(0, len(sampled_data)):
            cursor.execute(
                delete_query_geometry, (sampled_data[j][0], sampled_data[j][7])
            )
            db_connection.rollback()
        toc = time.perf_counter()
        geometry_query_delete_times.append(toc - tic)
    cursor.execute("TRUNCATE TABLE employees")
    print(f"""Average geometry query delete time of {num_rows}
        rows of random data from {faker_entries} employee entries
        took {sum(geometry_query_delete_times)/reps} seconds for a total of
        {sum(geometry_query_delete_times)} seconds using Python and psycopg2
        """)
    return geometry_query_delete_times


tic = time.perf_counter()
full_query_create_times = create_full_query(500, 500, 5000)
full_query_read_times = read_full_query(500, 500, 5000)
full_query_update_times = update_full_query(500, 500, 5000)
full_query_delete_times = delete_full_query(500, 500, 5000)
text_query_create_times = create_text_query(500, 500, 5000)
text_query_read_times = read_text_query(500, 500, 5000)
text_query_update_times = update_text_query(500, 500, 5000)
text_query_delete_times = delete_text_query(500, 500, 5000)
integer_query_create_times = create_int_query(500, 500, 5000)
integer_query_read_times = read_integer_query(500, 500, 5000)
integer_query_update_times = update_integer_query(500, 500, 5000)
integer_query_delete_times = delete_integer_query(500, 500, 5000)
float_query_create_times = create_float_query(500, 500, 5000)
float_query_read_times = read_float_query(500, 500, 5000)
float_query_update_times = update_float_query(500, 500, 5000)
float_query_delete_times = delete_float_query(500, 500, 5000)
json_query_create_times = create_json_query(500, 500, 5000)
json_query_read_times = read_json_query(500, 500, 5000)
json_query_update_times = update_json_query(500, 500, 5000)
json_query_delete_times = delete_json_query(500, 500, 5000)
bjson_query_create_times = create_bjson_query(500, 500, 5000)
bjson_query_read_times = read_bjson_query(500, 500, 5000)
bjson_query_update_times = update_bjson_query(500, 500, 5000)
bjson_query_delete_times = delete_bjson_query(500, 500, 5000)
geometry_query_create_times = create_geometry_query(500, 500, 5000)
geometry_query_read_times = read_geometry_query(500, 500, 5000)
geometry_query_update_times = update_geometry_query(500, 500, 5000)
geometry_query_delete_times = delete_geometry_query(500, 500, 5000)
toc = time.perf_counter()

print(f"This whole thing took {toc-tic} seconds to run")

# Add time results to a dataframe and output to excel file
df = pd.DataFrame(
    {
        "full_query_create": full_query_create_times,
        "full_query_read": full_query_read_times,
        "full_query_update": full_query_update_times,
        "full_query_delete": full_query_delete_times,
        "text_query_create": text_query_create_times,
        "text_query_read": text_query_read_times,
        "text_query_update": text_query_update_times,
        "text_query_delete": text_query_delete_times,
        "integer_query_create": integer_query_create_times,
        "integer_query_read": integer_query_read_times,
        "integer_query_update": integer_query_update_times,
        "integer_query_delete": integer_query_delete_times,
        "float_query_create": float_query_create_times,
        "float_query_read": float_query_read_times,
        "float_query_update": float_query_update_times,
        "float_query_delete": float_query_delete_times,
        "json_query_create": json_query_create_times,
        "json_query_read": json_query_read_times,
        "json_query_update": json_query_update_times,
        "json_query_delete": json_query_delete_times,
        "bjson_query_create": bjson_query_create_times,
        "bjson_query_read": bjson_query_read_times,
        "bjson_query_update": bjson_query_update_times,
        "bjson_query_delete": bjson_query_delete_times,
        "geometry_query_create": geometry_query_create_times,
        "geometry_query_read": geometry_query_read_times,
        "geometry_query_update": geometry_query_update_times,
        "geometry_query_delete": geometry_query_delete_times,
    }
)
df.to_excel("Python_output_final500.xlsx")
