# Spark_1 Assignment Drivers i.e. objects for functions in _utils_code

from spark_1.sample._utilis_code import *

# calling the Session
spark = Session()

# calling the user function to read the user csv
print("User Table: ")

u=user(spark)
u.show(truncate=False)
u.printSchema()

#calling the transaction function to read the transcation csv
print("Transcation Table: ")

t=transaction(spark)
t.show(truncate=False)
t.printSchema()

#calling the join_files
print("Table After joining user and Transcation tables")

j=join_files(u,t)
j.show(truncate=False)

# calling the product_bought
print("Find out products bought by each user: ")

p=product_bought(j)
p.show(truncate=False)

# calling the total_spending
print("Total spending done by each user on each product: ")

s=total_spending(j)
s.show(truncate=False)

# calling the uniq_loc
print("Count of unique locations where each product is sold: ")

l=uniq_loc(j)
l.show(truncate=False)