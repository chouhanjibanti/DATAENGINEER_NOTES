# main code 

from db_config import get_connection

def add_employee(cursor,connection):
    emp_id = int(input("Employee ID :"))
    name = input("Emmployee Name :")
    title = input("job Title :")
    salary = int(input("Salary :"))
    dept_id = int(input("Department ID :"))

    query = """ 
    INSERT  INTO EMPLOYEES (employee_id, employee_name, job_title, salary, department_id)
    VALUES (%s,%s,%s,%s,%s),
    """

    values = (emp_id,name,title,salary,dept_id)

    cursor.execute(query,values)
    connection.commit()

    print("Employee Added Successfully")

# ===============================================

def display_employees(cursor):
    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()

    if not rows:
        print("No employees found")

    for row in rows:
        print(row)

# =============================================

def find_employee_by_name(cursor):
    name = input("Enter name to search :")
    query = "SELECT * FROM EMPLOYEES WHERE employee_name = %s"
    cursor.execute(query,(name,))
    rows = cursor.fetchall()
    
    if not rows:
         print("No employees found")
    
    for row in rows:
        print(row)

# =============================================

def update_employee(cursor,connection):
    emp_id = int(input("Employee ID to update : "))
    new_salary = int(input("New Salary :"))

    query = "UPDATE EMPLOYEES SET SALARY = %s WHERE employee_id = %s "
    cursor.execute(query,(new_salary,emp_id))
    connection.commit()

    if cursor.rowcount == 0:
        print("No employee found with this id")
    else:
        print("Employee Updated Sucessfully....")


# =================================================

def delete_employee(cursor,connection):
    emp_id = int(input("Employee id to delete "))

    query = "DELETE from employees where employee_id=%s"
    cursor.execute(query,(emp_id,))
    connection.commit()

    if cursor.rowcount == 0:
        print("No employee found with this id")
    else:
        print("Employee Deleted Sucessfully....")
    

def main():

    connection = get_connection()

    cursor = connection.cursor()
    print("Database connected sucessfully....")


    while True:
        print("\n 1. Add Employee")
        print("2. View Employees")
        print("3. Find Employee")
        print("4. update Employee")
        print("5. Delete Employee")
        print("6. Exit")

        choice = int(input("Enter your choice"))

        if choice ==1:
            add_employee(cursor,connection)
        elif choice ==2:
            display_employees(cursor)
        elif choice ==3 :
            find_employee_by_name(cursor)
        elif choice ==4:
            update_employee(cursor,connection)
        elif choice == 5:
            delete_employee(cursor,connection)
        elif choice ==6 :
            break
        else:
            print("Invalid choice please try again")

    cursor.close()
    connection.close()
    print("Connection closed,TATA BYE BYE")


main()
            




    




# add_employee(cursor,connection)
# find_employee_by_name(cursor)
# update_employee(cursor,connection)
# delete_employee(cursor,connection)



