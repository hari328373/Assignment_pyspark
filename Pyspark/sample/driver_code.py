# driver code

from pyspark.sql import *
from Pyspark.sample._utilis_code import *

# calling the session
spark = Session()

# calling the table_1 to read the csv file
print("Table 1 :")
t1=table_1(spark)
t1.show(truncate=False) # display the table_1

# calling the convert_date
print("convert the Issue date to date type in Table 1 :")
d1 = convert_date(t1)
d1.show(truncate=False)

# calling the remove_space
print("Remove white spaces in Brand column :")
space_trim= remove_space(d1)
space_trim.show(truncate=False)

# calling the update_table
print("Replace Null values with empty and update Table 1 :")

update_table1 = update_table(space_trim)
update_table1.show(truncate=False)
update_table1.printSchema()  # returns the structure(contents) of the table

# calling the table 2 to read the csv file
print("Table 2:")
t2=table_2(spark)
t2.show(truncate=False)

# calling the change_case
print("change Camel case to Snake Case in Table 2: ")
T2 = change_case(t2)
T2.show(truncate=False)

# calling the convert_time
print("convert Time to unix seconds ")
st = convert_time(T2)
# st.printSchema()
st.show(truncate=False)


# calling the combine_table
print("combine Table 1 and Table 2 :")

u1 = update_table1.alias('u1')  # creating the alias names
ct=combine_table(u1,st)
ct.show(truncate=False)

# calling the get_EN
print(" get the country as EN: ")
EN = get_EN(ct)
EN.show(truncate=False)