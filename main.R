# Install packages if needed
# install.packages('RPostgres')
# install.packages("charlatan")
# install.packages("jsonlite")

# Imports
library(DBI)
library(charlatan)
library(jsonlite)
library(openxlsx)

# Establish connection to ExCompany database in PostgreSQL
con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'ExCompany',
                 host = 'localhost',
                 port = '5432',
                 user = 'postgres',
                 password = '<password>')  

# Define area code for company phone numbers
company_area_code <- 312

# Define the bounds for latitude and longitude of Chicago (where employees live)
chicago_latitude_bounds <- c(41.6445, 42.023)
chicago_longitude_bounds <- c(-87.9401, -87.524)

# Generate random data for each column
create_faker_data <- function(num_rows) {
  employee_data <- list()
  for (i in 1:num_rows) {
    
    # Generate random 9-digit employee ID
    employee_id <- sample(x = 100000000:999999999, size = 1, replace = FALSE)

    # Generate random first name and last name
    fake_name <- ch_name(1, messy = FALSE)
    fake_name_split <- strsplit(x = fake_name, split = " ")[[1]]
    
    if (length(fake_name_split) == 2) {
      first_name <- fake_name_split[1]
      last_name <- fake_name_split[2]
    }
    else if (length(fake_name_split) >= 3) {
      if (grepl("\\.", fake_name_split[1]) | fake_name_split[1] == "Miss") {
        first_name <- fake_name_split[2]
        last_name <- fake_name_split[3]
      } 
      else {
        first_name <- fake_name_split[1]
        last_name <- fake_name_split[2]
      }
    }
 
    # Generate random age between 18 and 65
    age <- as.integer(pmax(pmin(rnorm(1, mean = 35, sd = 10), 65), 18))
    
    # Generate random rating between 0 and 5
    rating <- pmax(0, pmin(round(rnorm(1, mean = 3, sd = 1), 2), 5))
    
    # Generate email based on name
    email <- paste(tolower(first_name), ".", tolower(last_name), "@company.com", sep = "")
    
    # Generate phone number with fixed area code and random extension number
    phone_number <- paste0(company_area_code, "-555-", sprintf("%04d", sample(x = 1000:9999, size = 1, replace = FALSE)))
    
    # Create JSON contact info
    json_contact_info <- toJSON(list(phone = phone_number, email = email))
    # Serialize BJSON contact info
    bjson_contact_info <- toJSON(list(phone = phone_number, email = email), auto_unbox = TRUE)
    
    # Generate random latitude and longitude within Chicago bounds
    latitude <- runif(1, min = chicago_latitude_bounds[1], max = chicago_latitude_bounds[2])
    longitude <- runif(1, min = chicago_longitude_bounds[1], max = chicago_longitude_bounds[2])
    
    # Create address string
    address <- paste("POINT(", longitude, " ", latitude, ")", sep = "")

    # Append employee data to list
    employee_data[[i]] <- list(employee_id, first_name, last_name, age, rating, json_contact_info, bjson_contact_info, address)
  }
  return(employee_data)
}

# Insert queries
create_full_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  
  # Row by row insertion using R and RPostgres of the whole dataset
  full_query_insert_times <- numeric(reps) 
  for (i in 1:reps) {
    employee_data <- create_faker_data(faker_entries)
    insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, age, rating, json_contact_info, bjson_contact_info, address)
                  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, insert_query_full, row)
    }
    toc <- Sys.time()
    full_query_insert_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average full table insertion of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(full_query_insert_times), "seconds for a total of",
    sum(full_query_insert_times), "seconds using R and RPostgres"
  ))
  return(full_query_insert_times)
}

create_text_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  
  # Row by row insertion using R and RPostgres of the text data types
  text_query_insert_times <- numeric(reps)
  for (i in 1:reps) {
    employee_data <- create_faker_data(faker_entries)
    insert_query_text <- "INSERT INTO employees (employee_id, first_name, last_name)
                  VALUES ($1, $2, $3)" 
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, insert_query_text, row[1:3])
    }
    toc <- Sys.time()
    text_query_insert_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average text insertion of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(text_query_insert_times), "seconds for a total of",
    sum(text_query_insert_times), "seconds using R and RPostgres"
  ))
  return(text_query_insert_times)
}

create_int_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  
  # Row by row insertion using R and RPostgres of the integer data types
  int_query_insert_times <- numeric(reps)
  for (i in 1:reps) {
    employee_data <- create_faker_data(faker_entries)
    insert_query_int <- "INSERT INTO employees (employee_id, age)
                  VALUES ($1, $2)" 
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, insert_query_int, c(row[1], row[4]))
    }
    toc <- Sys.time()
    int_query_insert_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average integer insertion of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(int_query_insert_times), "seconds for a total of",
    sum(int_query_insert_times), "seconds using R and RPostgres"
  ))
  return(int_query_insert_times)
}

create_float_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  
  # Row by row insertion using R and RPostgres of the float data types
  float_query_insert_times <- numeric(reps)
  for (i in 1:reps) {
    employee_data <- create_faker_data(faker_entries)
    insert_query_float <- "INSERT INTO employees (employee_id, rating)
                  VALUES ($1, $2)"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, insert_query_float, c(row[1], row[5]))
    }
    toc <- Sys.time()
    float_query_insert_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average float insertion of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(float_query_insert_times), "seconds for a total of",
    sum(float_query_insert_times), "seconds using R and RPostgres"
  ))
  return(float_query_insert_times)
}

create_json_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  
  # Row by row insertion using R and RPostgres of the json data types
  json_query_insert_times <- numeric(reps)
  for (i in 1:reps) {
    employee_data <- create_faker_data(faker_entries)
    insert_query_json <- "INSERT INTO employees (employee_id, json_contact_info)
                  VALUES ($1, $2)"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, insert_query_json, c(row[1], row[6]))
    }
    toc <- Sys.time()
    json_query_insert_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average json insertion of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(json_query_insert_times), "seconds for a total of",
    sum(json_query_insert_times), "seconds using R and RPostgres"
  ))
  return(json_query_insert_times)
}

create_bjson_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  
  # Row by row insertion using R and RPostgres of the bjson data types
  bjson_query_insert_times <- numeric(reps)
  for (i in 1:reps) {
    employee_data <- create_faker_data(faker_entries)
    insert_query_bjson <- "INSERT INTO employees (employee_id, bjson_contact_info)
                  VALUES ($1, $2)"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, insert_query_bjson, c(row[1], row[7]))
    }
    toc <- Sys.time()
    bjson_query_insert_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average bjson insertion of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(bjson_query_insert_times), "seconds for a total of",
    sum(bjson_query_insert_times), "seconds using R and RPostgres"
  ))
  return(bjson_query_insert_times)
}

create_geometry_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  
  # Row by row insertion using R and RPostgres of the geometry data types
  geometry_query_insert_times <- numeric(reps)
  for (i in 1:reps) {
    employee_data <- create_faker_data(faker_entries)
    insert_query_geometry <- "INSERT INTO employees (employee_id, address)
                  VALUES ($1, $2)"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, insert_query_geometry, c(row[1], row[8]))
    }
    toc <- Sys.time()
    geometry_query_insert_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average geometry insertion of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(geometry_query_insert_times), "seconds for a total of",
    sum(geometry_query_insert_times), "seconds using R and RPostgres"
  ))
  return(geometry_query_insert_times)
}

# Read queries
read_full_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement  full_query_read_times <- numeric(reps)
  full_query_read_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    select_query_full <- "SELECT employee_id, first_name, last_name, age,rating,
                          json_contact_info, bjson_contact_info, address
                          FROM employees 
                          WHERE employee_id = $1 AND first_name = $2 AND last_name = $3
            AND age = $4 AND rating = $5 AND json_contact_info::jsonb = $6
            AND bjson_contact_info::jsonb = $7 AND address::geometry = $8"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, select_query_full, row)
    }
    toc <- Sys.time()
    full_query_read_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average full query read time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(full_query_read_times), "seconds for a total of",
    sum(full_query_read_times), "seconds using R and RPostgres"
  ))
  return(full_query_read_times)
}

read_text_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  text_query_read_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    select_query_text <- "SELECT employee_id, first_name, last_name
                          FROM employees 
                          WHERE employee_id = $1 AND first_name = $2 AND last_name = $3"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, select_query_text, row[1:3])
    }
    toc <- Sys.time()
    text_query_read_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average full query read time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(text_query_read_times), "seconds for a total of",
    sum(text_query_read_times), "seconds using R and RPostgres"
  ))
  return(text_query_read_times)
}

read_integer_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  integer_query_read_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    select_query_integer <- "SELECT employee_id, age
                          FROM employees 
                          WHERE employee_id = $1 AND age = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, select_query_integer, c(row[1], row[4]))
    }
    toc <- Sys.time()
    integer_query_read_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average integer query read time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(integer_query_read_times), "seconds for a total of",
    sum(integer_query_read_times), "seconds using R and RPostgres"
  ))
  return(integer_query_read_times)
}

read_float_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  float_query_read_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    select_query_float <- "SELECT employee_id, rating
                          FROM employees 
                          WHERE employee_id = $1 AND rating = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, select_query_float, c(row[1], row[5]))
    }
    toc <- Sys.time()
    float_query_read_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average float query read time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(float_query_read_times), "seconds for a total of",
    sum(float_query_read_times), "seconds using R and RPostgres"
  ))
  return(float_query_read_times)
}

read_json_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  json_query_read_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    select_query_json <- "SELECT employee_id, json_contact_info
                          FROM employees 
                          WHERE employee_id = $1 AND json_contact_info::jsonb = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, select_query_json, c(row[1], row[6]))
    }
    toc <- Sys.time()
    json_query_read_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average json query read time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(json_query_read_times), "seconds for a total of",
    sum(json_query_read_times), "seconds using R and RPostgres"
  ))
  return(json_query_read_times)
}

read_bjson_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  bjson_query_read_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    select_query_bjson <- "SELECT employee_id, bjson_contact_info
                          FROM employees 
                          WHERE employee_id = $1 AND bjson_contact_info::jsonb = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, select_query_bjson, c(row[1], row[7]))
    }
    toc <- Sys.time()
    bjson_query_read_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average bjson query read time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(bjson_query_read_times), "seconds for a total of",
    sum(bjson_query_read_times), "seconds using R and RPostgres"
  ))
  return(bjson_query_read_times)
}

read_geometry_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  geometry_query_read_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    select_query_geometry <- "SELECT employee_id, address
                          FROM employees 
                          WHERE employee_id = $1 AND address::geometry = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, select_query_geometry, c(row[1], row[8]))
    }
    toc <- Sys.time()
    geometry_query_read_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average geometry query read time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(geometry_query_read_times), "seconds for a total of",
    sum(geometry_query_read_times), "seconds using R and RPostgres"
  ))
  return(geometry_query_read_times)
}

# Update queries
update_full_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  full_query_update_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  initial_samples <- sample(x = employee_data, size = num_rows, replace = FALSE)
  for (i in 1:reps) {
    update_query_full <- "UPDATE employees
                          SET first_name = $1, last_name = $2, age = $3, rating = $4,
                          json_contact_info = $5, bjson_contact_info = $6, address = $7
                          WHERE employee_id = $8 AND first_name = $9 AND last_name = $10
                          AND age = $11 AND rating = $12 AND json_contact_info::jsonb = $13
                          AND bjson_contact_info::jsonb = $14 AND address::geometry = $15"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (j in 1:length(sampled_data)) {
      dbExecute(con, update_query_full, c(
          initial_samples[[j]][[2]],  # Accessing first_name from initial_samples
          initial_samples[[j]][[3]],  # Accessing last_name from initial_samples
          initial_samples[[j]][[4]],  # Accessing age from initial_samples
          initial_samples[[j]][[5]],  # Accessing rating from initial_samples
          initial_samples[[j]][[6]],  # Accessing json_contact_info from initial_samples
          initial_samples[[j]][[7]],  # Accessing bjson_contact_info from initial_samples
          initial_samples[[j]][[8]],  # Accessing address from initial_samples
          sampled_data[[j]][[1]],     # Accessing employee_id from sampled_data
          sampled_data[[j]][[2]],     # Accessing first_name from sampled_data
          sampled_data[[j]][[3]],     # Accessing last_name from sampled_data
          sampled_data[[j]][[4]],     # Accessing age from sampled_data
          sampled_data[[j]][[5]],     # Accessing rating from sampled_data
          sampled_data[[j]][[6]],     # Accessing json_contact_info from sampled_data
          sampled_data[[j]][[7]],     # Accessing bjson_contact_info from sampled_data
          sampled_data[[j]][[8]]      # Accessing address from sampled_data
        ))
      }
    toc <- Sys.time()
    full_query_update_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average full query update time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(full_query_update_times), "seconds for a total of",
    sum(full_query_update_times), "seconds using R and RPostgres"
  ))
  return(full_query_update_times)
}

update_text_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  text_query_update_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  initial_samples <- sample(x = employee_data, size = num_rows, replace = FALSE)
  for (i in 1:reps) {
    update_query_text <- "UPDATE employees
                          SET first_name = $1, last_name = $2
                          WHERE employee_id = $3 AND first_name = $4 AND last_name = $5"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (j in 1:length(sampled_data)) {
      dbExecute(con, update_query_text, c(
        initial_samples[[j]][[2]],  # Accessing first_name from initial_samples
        initial_samples[[j]][[3]],  # Accessing last_name from initial_samples
        sampled_data[[j]][[1]],     # Accessing employee_id from sampled_data
        sampled_data[[j]][[2]],     # Accessing first_name from sampled_data
        sampled_data[[j]][[3]]      # Accessing last_name from sampled_data
      ))
    }
    toc <- Sys.time()
    text_query_update_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average text query update time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(text_query_update_times), "seconds for a total of",
    sum(text_query_update_times), "seconds using R and RPostgres"
  ))
  return(text_query_update_times)
}

update_integer_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  integer_query_update_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  initial_samples <- sample(x = employee_data, size = num_rows, replace = FALSE)
  for (i in 1:reps) {
    update_query_integer <- "UPDATE employees
                          SET age = $1
                          WHERE employee_id = $2 AND age = $3"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (j in 1:length(sampled_data)) {
      dbExecute(con, update_query_integer, c(
        initial_samples[[j]][[4]],  # Accessing age from initial_samples
        sampled_data[[j]][[1]],     # Accessing employee_id from sampled_data
        sampled_data[[j]][[4]]      # Accessing age from sampled_data
      ))
    }
    toc <- Sys.time()
    integer_query_update_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average integer query update time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(integer_query_update_times), "seconds for a total of",
    sum(integer_query_update_times), "seconds using R and RPostgres"
  ))
  return(integer_query_update_times)
}

update_float_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  float_query_update_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  initial_samples <- sample(x = employee_data, size = num_rows, replace = FALSE)
  for (i in 1:reps) {
    update_query_float <- "UPDATE employees
                          SET rating = $1
                          WHERE employee_id = $2 AND rating = $3"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (j in 1:length(sampled_data)) {
      dbExecute(con, update_query_float, c(
        initial_samples[[j]][[5]],  # Accessing rating from initial_samples
        sampled_data[[j]][[1]],     # Accessing employee_id from sampled_data
        sampled_data[[j]][[5]]      # Accessing rating from sampled_data
      ))
    }
    toc <- Sys.time()
    float_query_update_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average float query update time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(float_query_update_times), "seconds for a total of",
    sum(float_query_update_times), "seconds using R and RPostgres"
  ))
  return(float_query_update_times)
}

update_json_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  json_query_update_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  initial_samples <- sample(x = employee_data, size = num_rows, replace = FALSE)
  for (i in 1:reps) {
    update_query_json <- "UPDATE employees
                          SET json_contact_info = $1
                          WHERE employee_id = $2 AND json_contact_info::jsonb = $3"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (j in 1:length(sampled_data)) {
      dbExecute(con, update_query_json, c(
        initial_samples[[j]][[6]],  # Accessing json_contact_info from initial_samples
        sampled_data[[j]][[1]],     # Accessing employee_id from sampled_data
        sampled_data[[j]][[6]]      # Accessing json_contact_info from sampled_data
      ))
    }
    toc <- Sys.time()
    json_query_update_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average json query update time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(json_query_update_times), "seconds for a total of",
    sum(json_query_update_times), "seconds using R and RPostgres"
  ))
  return(json_query_update_times)
}

update_bjson_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  bjson_query_update_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  initial_samples <- sample(x = employee_data, size = num_rows, replace = FALSE)
  for (i in 1:reps) {
    update_query_bjson <- "UPDATE employees
                          SET bjson_contact_info = $1
                          WHERE employee_id = $2 AND bjson_contact_info::jsonb = $3"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (j in 1:length(sampled_data)) {
      dbExecute(con, update_query_bjson, c(
        initial_samples[[j]][[7]],  # Accessing bjson_contact_info from initial_samples
        sampled_data[[j]][[1]],     # Accessing employee_id from sampled_data
        sampled_data[[j]][[7]]      # Accessing bjson_contact_info from sampled_data
      ))
    }
    toc <- Sys.time()
    bjson_query_update_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average bjson query update time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(bjson_query_update_times), "seconds for a total of",
    sum(bjson_query_update_times), "seconds using R and RPostgres"
  ))
  return(bjson_query_update_times)
}

update_geometry_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  geometry_query_update_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  initial_samples <- sample(x = employee_data, size = num_rows, replace = FALSE)
  for (i in 1:reps) {
    update_query_geometry <- "UPDATE employees
                          SET address = $1
                          WHERE employee_id = $2 AND address::geometry = $3"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (j in 1:length(sampled_data)) {
      dbExecute(con, update_query_geometry, c(
        initial_samples[[j]][[8]],  # Accessing address from initial_samples
        sampled_data[[j]][[1]],     # Accessing employee_id from sampled_data
        sampled_data[[j]][[8]]      # Accessing address from sampled_data
      ))
    }
    toc <- Sys.time()
    geometry_query_update_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average geometry query update time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(geometry_query_update_times), "seconds for a total of",
    sum(geometry_query_update_times), "seconds using R and RPostgres"
  ))
  return(geometry_query_update_times)
}

# Delete queries
delete_full_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  full_query_delete_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    delete_query_full <- "DELETE FROM employees
                          WHERE employee_id = $1 AND first_name = $2 AND last_name = $3
                          AND age = $4 AND rating = $5 AND json_contact_info::jsonb = $6
                          AND bjson_contact_info::jsonb = $7 AND address::geometry = $8"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, delete_query_full, row)
    }
    toc <- Sys.time()
    full_query_delete_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average full query delete time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(full_query_delete_times), "seconds for a total of",
    sum(full_query_delete_times), "seconds using R and RPostgres"
  ))
  return(full_query_delete_times)
}

delete_text_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  text_query_delete_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    delete_query_text <- "DELETE FROM employees
                          WHERE employee_id = $1 AND first_name = $2 AND last_name = $3"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, delete_query_text, row[1:3])
    }
    toc <- Sys.time()
    text_query_delete_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average text query delete time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(text_query_delete_times), "seconds for a total of",
    sum(text_query_delete_times), "seconds using R and RPostgres"
  ))
  return(text_query_delete_times)
}

delete_integer_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  integer_query_delete_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    delete_query_integer <- "DELETE FROM employees
                          WHERE employee_id = $1 AND age = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, delete_query_integer, c(row[1], row[4]))
    }
    toc <- Sys.time()
    integer_query_delete_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average integer query delete time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(integer_query_delete_times), "seconds for a total of",
    sum(integer_query_delete_times), "seconds using R and RPostgres"
  ))
  return(integer_query_delete_times)
}

delete_float_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  float_query_delete_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    delete_query_float <- "DELETE FROM employees
                          WHERE employee_id = $1 AND rating = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, delete_query_float, c(row[1], row[5]))
    }
    toc <- Sys.time()
    float_query_delete_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average float query delete time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(float_query_delete_times), "seconds for a total of",
    sum(float_query_delete_times), "seconds using R and RPostgres"
  ))
  return(float_query_delete_times)
}

delete_json_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  json_query_delete_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    delete_query_json <- "DELETE FROM employees
                          WHERE employee_id = $1 AND json_contact_info::jsonb = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, delete_query_json, c(row[1], row[6]))
    }
    toc <- Sys.time()
    json_query_delete_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average json query delete time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(json_query_delete_times), "seconds for a total of",
    sum(json_query_delete_times), "seconds using R and RPostgres"
  ))
  return(json_query_delete_times)
}

delete_bjson_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  bjson_query_delete_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    delete_query_bjson <- "DELETE FROM employees
                          WHERE employee_id = $1 AND bjson_contact_info::jsonb = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, delete_query_bjson, c(row[1], row[7]))
    }
    toc <- Sys.time()
    bjson_query_delete_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average bjson query delete time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(bjson_query_delete_times), "seconds for a total of",
    sum(bjson_query_delete_times), "seconds using R and RPostgres"
  ))
  return(bjson_query_delete_times)
}

delete_geometry_query <- function(reps, num_rows, faker_entries) {
  # reps {int}: number of repetitions of simulating the insertion
  # num_rows {int}: number of rows to add to the employees table
  # faker_entries {int}: number of fake entries to sample from without replacement
  geometry_query_delete_times <- numeric(reps)
  employee_data <- create_faker_data(faker_entries)
  insert_query_full <- "INSERT INTO employees (employee_id, first_name, last_name, 
                        age, rating, json_contact_info, bjson_contact_info, address)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)" 
  for (row in employee_data) {
    dbExecute(con, insert_query_full, row)
  }
  for (i in 1:reps) {
    delete_query_geometry <- "DELETE FROM employees
                          WHERE employee_id = $1 AND address::geometry = $2"
    sampled_data <- sample(x = employee_data, size = num_rows, replace = FALSE)
    tic <- Sys.time()
    for (row in sampled_data) {
      dbExecute(con, delete_query_geometry, c(row[1], row[8]))
    }
    toc <- Sys.time()
    geometry_query_delete_times[i] <- as.numeric(difftime(toc, tic, units = "secs"))
    dbExecute(con, "TRUNCATE TABLE employees")
  }
  cat(paste(
    "Average geometry query delete time of", num_rows,
    "rows of random data from", faker_entries, "employee entries",
    "took", mean(geometry_query_delete_times), "seconds for a total of",
    sum(geometry_query_delete_times), "seconds using R and RPostgres"
  ))
  return(geometry_query_delete_times)
}

tic <- Sys.time()
full_query_create_times <- create_full_query(1000, 1000, 10000)
full_query_read_times <- read_full_query(1000, 1000, 10000)
full_query_update_times <- update_full_query(1000, 1000, 10000)
full_query_delete_times <- delete_full_query(1000, 1000, 10000)
text_query_create_times <- create_text_query(1000, 1000, 10000)
text_query_read_times <- read_text_query(1000, 1000, 10000)
text_query_update_times <- update_text_query(1000, 1000, 10000)
text_query_delete_times <- delete_text_query(1000, 1000, 10000)
integer_query_create_times <- create_int_query(1000, 1000, 10000)
integer_query_read_times <- read_integer_query(1000, 1000, 10000)
integer_query_update_times <- update_integer_query(1000, 1000, 10000)
integer_query_delete_times <- delete_integer_query(1000, 1000, 10000)
float_query_create_times <- create_float_query(1000, 1000, 10000)
float_query_read_times <- read_float_query(1000, 1000, 10000)
float_query_update_times <- update_float_query(1000, 1000, 10000)
float_query_delete_times <- delete_float_query(1000, 1000, 10000)
json_query_create_times <- create_json_query(1000, 1000, 10000)
json_query_read_times <- read_json_query(1000, 1000, 10000)
json_query_update_times <- update_json_query(1000, 1000, 10000)
json_query_delete_times <- delete_json_query(1000, 1000, 10000)
bjson_query_create_times <- create_bjson_query(1000, 1000, 10000)
bjson_query_read_times <- read_bjson_query(1000, 1000, 10000)
bjson_query_update_times <- update_bjson_query(1000, 1000, 10000)
bjson_query_delete_times <- delete_bjson_query(1000, 1000, 10000)
geometry_query_create_times <- create_geometry_query(1000, 1000, 10000)
geometry_query_read_times <- read_geometry_query(1000, 1000, 10000)
geometry_query_update_times <- update_geometry_query(1000, 1000, 10000)
geometry_query_delete_times <- delete_geometry_query(1000, 1000, 10000)
toc <- Sys.time()

cat(paste("This whole thing took", as.numeric(difftime(toc, tic, units = "secs")), "seconds to run"))

# Add time results to a dataframe and output to Excel file
df <- data.frame(
  full_query_create = full_query_create_times,
  full_query_read = full_query_read_times,
  full_query_update = full_query_update_times,
  full_query_delete = full_query_delete_times,
  text_query_create = text_query_create_times,
  text_query_read = text_query_read_times,
  text_query_update = text_query_update_times,
  text_query_delete = text_query_delete_times,
  integer_query_create = integer_query_create_times,
  integer_query_read = integer_query_read_times,
  integer_query_update = integer_query_update_times,
  integer_query_delete = integer_query_delete_times,
  float_query_create = float_query_create_times,
  float_query_read = float_query_read_times,
  float_query_update = float_query_update_times,
  float_query_delete = float_query_delete_times,
  json_query_create = json_query_create_times,
  json_query_read = json_query_read_times,
  json_query_update = json_query_update_times,
  json_query_delete = json_query_delete_times,
  bjson_query_create = bjson_query_create_times,
  bjson_query_read = bjson_query_read_times,
  bjson_query_update = bjson_query_update_times,
  bjson_query_delete = bjson_query_delete_times,
  geometry_query_create = geometry_query_create_times,
  geometry_query_read = geometry_query_read_times,
  geometry_query_update = geometry_query_update_times,
  geometry_query_delete = geometry_query_delete_times
)

write.xlsx(df, "R_output.xlsx", rowNames = FALSE)