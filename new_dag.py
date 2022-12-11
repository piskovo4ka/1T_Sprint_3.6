from datetime import datetime, timedelta
import psycopg2

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from random import random 
from random import randrange
import os

# Переменная с именем txt файла
file_path = Variable.get("file_path")
    
# ф-я для передачи креденшиалс    
def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    conn_to_airflow = BaseHook.get_connection(conn_id)
    return conn_to_airflow

# просто тренируемся
def hello():
    
    print("Hello Sultan!")

# просто прощаемся
def buy():
    
    print("Buy, buy, Sultan!")    

# ф-я для генерации и записи двух чисел в одну строку
# а также инициализация и итерация счетчика запусков daga
def write_decimal():

    a = randrange(10)
    b = randrange(10)    
    file_path = Variable.get("file_path")

    c_run = int(Variable.get("count_run"))
    
    file = open(file_path, "a")  
    file.close()
    
    with open(file_path, 'r+' ) as f: 
        f.seek(0)
        lines = f.readlines()
        f.truncate()
        f.seek(0)

               
        print('len=', len(lines))

        if int(len(lines)) > 0:
            print('было', lines[:-1])
            f.writelines(lines[:-1])

            Variable.update(key="count_run", value=int(len(lines)))

        elif int(len(lines)) == 0:
            Variable.update(key="count_run", value=int(len(lines))+1)    

        print('c_run', c_run)

        print(f'num_1= {a} num_2= {b}')    
        f.write(f'{a} {b} \n')


# ф-я для просмотра содержимого файла    
def show():
    file_path = Variable.get("file_path")
    
    with open(file_path, 'r+' ) as f:    
        print(*f)

# ф-я для подсчета сумм в колонках и записи их разности в конец файла
def read_decimal():

    file_path = Variable.get("file_path")

    with open(file_path, 'a+' ) as f:
        f.seek(0)
        lines = f.readlines()
        s1, s2 = 0, 0
        for line in lines:
            print(line)
            x1, x2 = line.strip().split(" ")
            s1 += int(x1)
            s2 += int(x2)

        diff = s1 - s2
        print(f's_1= {s1} s_2= {s2} diff= {diff}') 
        f.write(str(diff)+"\n")
        
# ф-я для проверки корректности работы (в соответствии с условием задания)
def check_decimal(**kwargs):
    ti = kwargs['ti']
    
    file_path = Variable.get("file_path")

    x, s_1, s_2, diff_res= 0, 0, 0, 0
    file_ex, count_res, summ_res= False, False, False
    if os.path.exists(file_path):
        file_ex = True
        c_run = int(Variable.get("count_run"))
        with open(file_path, "r+") as f:
            for line in f.readlines():
                x += 1
                split = line.strip().split(" ")
                if len(split) > 1:
                    x_1, x_2 = split
                    s_1 += int(x_1)
                    s_2 += int(x_2)
                else:
                    diff_res = int(split[0])
        print(f'число строк в файле = {x}, число запусков = {c_run}')
        print(f'разница сумм = {s_1-s_2}, diff_res = {diff_res}')
        if (x - 1) == c_run:
            count_res = True
        if diff_res == (s_1-s_2):
            summ_res = True

    if file_ex and count_res and summ_res:
        ti.xcom_push(value=[True, file_ex, count_res, summ_res], key='file_is_ok')
        return True
    else:
        ti.xcom_push(value=[False, file_ex, count_res, summ_res], key='file_is_ok')
        return False\

# оператор ветвления
def brash_sensor_choise(**kwargs):
    ti = kwargs['ti']

    file_is_ok, _, _, _ = ti.xcom_pull(key='file_is_ok', task_ids='my_sensor')

    if file_is_ok:
        return 'connect_to_psql'
    else:
        return 'my_exception'  
                
# ф-я исключения в случае неудачи 
def my_exception(**kwargs):
    ti = kwargs['ti']

    file_is_ok, _, _, _ = ti.xcom_pull(key='file_is_ok', task_ids='my_sensor')

    print(f'Что-то пошло не так! =(((')

# ф-я подключения к Postgres в случае удачи =) 
def connect_to_psql(**kwargs):
    ti = kwargs['ti']

    file_path = Variable.get("file_path")
    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port,\
                                                             conn_to_airflow.login, conn_to_airflow.password,\
                                                                 conn_to_airflow.schema
    
    ti.xcom_push(value = [pg_hostname, pg_port, pg_username, pg_pass, pg_db], key='conn_to_airflow')
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS n_table (id serial PRIMARY KEY, x_1 integer, x_2 integer);")
    
    with open(file_path, "r+") as f:
        for line in f.readlines():
            split = line.strip().split(" ")
            if len(split) > 1:
                x_1, x_2 = split
                cursor.execute("INSERT INTO n_table (x_1, x_2) VALUES (%s, %s)", (x_1, x_2))
    
    #cursor.fetchall()
    pg_conn.commit()

    cursor.close()
    pg_conn.close()

# ф-я проверки, что все работает и вывода всех строк в logs 
def read_from_psql(**kwargs):
    ti = kwargs['ti']
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = ti.xcom_pull(key='conn_to_airflow', task_ids='connect_to_psql')

    # pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port,\
    #                                                          pg_conn.login, pg_conn.password, pg_conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    cursor = pg_conn.cursor()

    cursor.execute("SELECT * FROM n_table;")
    print(cursor.fetchone())
    
    cursor.close()
    pg_conn.close() 
         
# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="new_dag", start_date=datetime(2022, 12, 11), schedule = "* * * * *", \
         max_active_runs=5) as dag:
    
    create_col_task = PostgresOperator(
        task_id="new_column",
        sql=""" ALTER TABLE n_table ADD COLUMN IF NOT EXISTS diff integer;
                UPDATE n_table SET diff = x_1 - x_2;
              """,
        postgres_conn_id=Variable.get("conn_id"),
        trigger_rule='one_success',
        dag=dag
    )

    
    # Tasks are represented as operators
    bash_task_buy = BashOperator(task_id="buy", bash_command="echo buy", do_xcom_push=False)
    python_task = PythonOperator(task_id="world", python_callable = hello, do_xcom_push=False)
    
    #python_task_variables = PythonOperator(task_id="variables", python_callable = variables)
    python_task_wr_decimal = PythonOperator(task_id="write_decimal", python_callable = write_decimal)
    python_task_read_decimal = PythonOperator(task_id="read_decimal", python_callable = read_decimal)
    python_task_show = PythonOperator(task_id="show", python_callable = show)
    my_exception_task = PythonOperator(task_id="my_exception", python_callable=my_exception)
    conn_to_psql_tsk = PythonOperator(task_id="connect_to_psql", python_callable = connect_to_psql)
    read_from_psql_tsk = PythonOperator(task_id="read_from_psql", python_callable = read_from_psql)
    

    my_sensor_tsk = PythonSensor(
        task_id='my_sensor',
        poke_interval=6,
        timeout=60,
        mode="reschedule",
        python_callable=check_decimal,
        dag=dag,
        do_xcom_push=True
    )

    brash_sensor_choise_tsk = BranchPythonOperator(
        task_id='bash_sensor_choise',
        python_callable=brash_sensor_choise,
        do_xcom_push=False
    )


    # Set dependencies between tasks
    python_task >> python_task_wr_decimal >> python_task_show  \
         >> python_task_read_decimal >> my_sensor_tsk >> brash_sensor_choise_tsk \
            >> [conn_to_psql_tsk, my_exception_task] >> create_col_task >> read_from_psql_tsk >> bash_task_buy 