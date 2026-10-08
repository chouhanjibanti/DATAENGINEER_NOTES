Pyspark functions :- 

1. Easy Level :- 

1.  Load the CSV file into a DataFrame, print its schema, and print the total number of rows and columns.
2. Show the first 5 rows, but only for the columns `brand`, `model` and `seats`.
3. Rename the column `efficiency_wh_per_km` to `efficiency`, and drop the columns `fast_charge_port` and `source_url`.


2. Filtering & Selection :- 

1. Find all vehicles of the brand `"Tesla"`. Show `brand`, `model` and `range_km`.
2. Find all vehicles that have exactly 7 seats **and** a range greater than 400 km.
3. Find all vehicles whose brand is `"Audi"`, `"BMW"` or `"Tesla"`.


3. Aggregations
1. Find the minimum and maximum `range_km` of the whole dataset.
2. For each `car_body_type`, find the number of vehicles and the average `battery_capacity_kWh` (rounded to 2 decimals).
3. Find the brands whose average `top_speed_kmh` is greater than 200. Show the fastest brand first.

4. Sorting
1.  Show the top 5 vehicles with the highest `top_speed_kmh`.
2. Sort the vehicles by `brand` (A to Z) and then by `range_km` (highest first).
3. Sort the vehicles by `acceleration_0_100_s` (fastest first) and keep the rows with null acceleration at the **end**.


5. Handling Nulls
5.1 How many vehicles have a null value in `cargo_volume_l`?
5.2 Drop only those rows where `torque_nm` **or** `range_km` is null. Compare the row count before and after.
5.3 Replace the null values in `seats` with `5` and the null values in `fast_charge_port` with `"Unknown"` (do both in one step).


6. Distinct & Deduplication
6.1 Show all the different values of the `drivetrain` column.
6.2 How many different brands are there? How many different (`brand`, `car_body_type`) combinations are there?
6.3 Remove duplicate rows based only on the columns `brand` and `model`. Compare the row counts.

7. String & Column Operations
7.1 Add two new columns: `brand_upper` (brand in capital letters) and `model_length` (number of characters in the model name).
7.2 Add a column `range_level`: `"Long"` if `range_km` > 400, `"Medium"` if `range_km` > 250, otherwise `"Short"`.
7.3 Create a column `brand_code` with the first 3 letters of the brand in capital letters (for example `Tesla` becomes `TES`).


8. Type Casting
8.1 Convert the column `seats` into a string column. Show the data types to prove it.
8.2 Convert `top_speed_kmh` into a double (decimal) column.
8.3 Convert both `torque_nm` and `number_of_cells` into integers using a loop.


9. Basic UDFs
9.1 Create a pandas UDF `seat_category` that returns `"Family"` if the car has 6 or more seats, otherwise `"Compact"`.
9.2 Create a pandas UDF that converts `range_km` into miles (1 km = 0.621371 miles) and add it as a new column `range_miles`.
9.3 Create a pandas UDF with **two** input columns that calculates `range_per_kwh = range_km / battery_capacity_kWh`.

10. More Selections
10.1 Add a True/False column `is_long_range` that is `True` when `range_km` is greater than 400.
10.2 Using `selectExpr`, show `brand`, `model`, the range in miles as `range_miles`, and the car volume as `volume_m3` (`length_mm * width_mm * height_mm`, converted to cubic metres).
10.3 Show all vehicles whose `car_body_type` is **not** `"SUV"`.


11. DataFrame Metadata
11.1 Show all column names together with their data types.
11.2 Print the number of rows and the number of columns of the DataFrame as `(rows, columns)`.
11.3 Show summary statistics (count, mean, stddev, min, max) of `range_km` and `top_speed_kmh`.



============================================================================

# MEDIUM LEVEL


12. Window Functions & Advanced Aggregations
12.1 Show the top 3 fastest vehicles (by `top_speed_kmh`) inside each `car_body_type`.
12.2 Add a column with the average `range_km` of each brand, and another column showing how far each vehicle is from its brand average.
12.3 For each brand, sort the vehicles by `range_km` and show the range of the **previous** vehicle in a column `prev_range`.


13. Pivoting
13.1 Create a table where each row is a `car_body_type`, each column is a `drivetrain`, and the values are the number of vehicles.
13.2 Show the average `top_speed_kmh` for each `segment` as rows, with only the body types `"SUV"`, `"Hatchback"` and `"Sedan"` as columns.
13.3 For each `brand`, count the vehicles having 2, 4, 5 and 7 seats. Replace the null values in the result with `0`.



14. Joins
14.1 Join `df_country` with the EV DataFrame on `brand` using a **left join** (keep all EV rows).
14.2 Do an **inner join** and count how many vehicles come from each `country`.
14.3 Find the vehicles whose brand is **not** present in `df_country`.


15. Complex Filtering & Conditions
15.1 Find the SUVs that have a `range_km` above 350 **and** a `top_speed_kmh` of at least 180.
15.2 Find the vehicles that accelerate 0-100 in **less than 4 seconds** OR have a top speed **above 250 km/h**.
15.3 Using SQL, find the vehicles whose brand starts with the letter `T`, with range above 300 km, sorted by range (highest first).


16. JSON Export & Schema
16.1 Write the whole DataFrame to a JSON folder named `ev_json`. Overwrite it if it already exists.
16.2 Read the `ev_json` folder back **without** giving a schema and print the schema Spark inferred.
16.3 Save only `brand`, `model`, `range_km` and `seats` to a JSON folder `ev_json_small`. Read it again using a **schema that you define manually** with `StructType`.


17. Performance Optimization
17.1 Cache the DataFrame, run a `groupBy`, then remove it from the cache. Print `df.is_cached` after each step.
17.2 Print the number of partitions of the DataFrame. Then change it to 4 partitions with `repartition`, and to 1 partition with `coalesce`.
17.3 Print the execution plan of a query that filters SUVs and counts them per brand. (Hint: use `explain()`.)


18. Nested Column Creation
18.1 Create a struct column `dimensions` that holds `length_mm`, `width_mm` and `height_mm`.
18.2 From the `dimensions` struct, select only `length_mm`. Then flatten the whole struct back into separate columns.
18.3 Create a struct `charging` with two fields named `power_kw` (from `fast_charging_power_kw_dc`) and `port` (from `fast_charge_port`). Then keep only the vehicles with `charging.power_kw` above 100.


19. Complex Sorting
19.1 Sort the vehicles by `seats` (highest first), and when seats are equal, by `range_km` (highest first).
19.2 Sort by `brand` (A to Z), then by `top_speed_kmh` (highest first, nulls at the end).
19.3 Create a column `range_per_kwh` (`range_km / battery_capacity_kWh`), remove rows where it is null, and show the 10 most efficient vehicles.
