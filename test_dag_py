
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException
import pandas as pd



def _print_context(ds, execution_date, variable, **context):
    print(f"------------- exec dttm = {execution_date} and minute = {execution_date.minute}")
    print(f"------------- ds = {ds} and type of (ds) = {type(ds)}")
    print(context.keys())
    print(context)
    return variable

def _pick_erp_system(**kwargs):
    if kwargs['execution_date'].minute % 2 == 0:
        return "read_data"
    else:
        return "fetch_new_sales"
    
def _read_data(**kwargs):
    df = pd.read_csv('data/data.csv')
    kwargs['task_instance'].xcom_push(key='df', value=df)

def _transform_data_(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='read_data', key='df')
    df.item_price = df.item_price.apply(lambda x: x.strip('$').strip(' ')).astype('float')
    group_ds = df.groupby('item_name',as_index=False)['item_price'].agg('mean').round(2)
    kwargs['task_instance'].xcom_push(key='group_ds', value=group_ds)

def _deploy_model(**kwargs):
    group_ds = kwargs['task_instance'].xcom_pull(task_ids='transform_data_', key='group_ds')
    group_ds.to_csv('data/group_ds.csv',index=False)

def _clean_new_sales():
    return f"_clean_new_sales"

def _clean_wheather():
    return f"_clean_wheather"

def _fetch_wheather():
    return f"_fetch_wheather"

def _fetch_new_sales():
    return f"_fetch_new_sales"

def _join_datasets():
    return f"_join_sales"

def _train_model(**kwargs):
    print(kwargs['ds_nodash'])
    print(kwargs['logical_date']) 

def _latest_only(**kwargs):
    left_window = kwargs['dag'].following_schedule(kwargs['logical_date'])
    right_window = kwargs['dag'].following_schedule(left_window)
    
    now = pendulum.now()
    print(f'Left window was {left_window}')
    print(f'Now was {now}')
    print(f'Right window was {right_window}')
    if not left_window < now <= right_window:
        raise AirflowSkipException('Not the most recent run')

# def _deploy(**kwargs):
#     import datetime as dt
#     if dt.datetime.strptime(kwargs['ds_nodash'],'%Y%m%d').date() == dt.date.today():
#         _deploy_model()
#     return kwargs['ds_nodash']


# date = '20230817'
# prev_date = '20230816'
# datetime.strptime(date,'%Y%m%d').date() < datetime.strptime(prev_date,'%Y%m%d').date()

with DAG(
    dag_id='branching',
    schedule_interval='0 */2 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    # end_date=pendulum.datetime(2023, 9, 1, tz="UTC"),
    catchup=False
) as dag:
    print_context = PythonOperator(
        task_id='print_context',
        python_callable=_print_context,
        op_kwargs = {'variable':'value_of_variable'}
    )
    
    start = DummyOperator(task_id = 'start')

    # rm_file = BashOperator(
    # task_id="rm_file",
    # bash_command="rm opt/airflow/data/group_ds.csv ",
    # )
    
    pick_erp_system = BranchPythonOperator(
        task_id = 'pick_erp_system',
        python_callable=_pick_erp_system
    )

    read_data = PythonOperator(
        task_id='read_data',
        python_callable=_read_data
    )
    
    fetch_new_sales = PythonOperator(
        task_id='fetch_new_sales',
        python_callable=_fetch_new_sales
    )

    transform_data_ = PythonOperator(
        task_id='transform_data_',
        python_callable=_transform_data_
    )
    
    clean_new_sales = PythonOperator(
        task_id='clean_new_sales',
        python_callable=_clean_new_sales
    )

    clean_wheather = PythonOperator(
        task_id='clean_wheather',
        python_callable=_clean_wheather
    )

    fetch_wheather = PythonOperator(
        task_id='fetch_wheather',
        python_callable=_fetch_wheather
    )

    join_datasets = PythonOperator(
        task_id='join_datasets',
        python_callable=_join_datasets,
    )

    join_erp_branch = DummyOperator(
        task_id='join_erp_branch',
        trigger_rule = 'none_failed'
    )

    train_model = PythonOperator(
        task_id='train_model',
        python_callable=_train_model
    )

    deploy_model = PythonOperator(
        task_id='deploy_model',
        python_callable=_deploy_model
    )

    latest_only = PythonOperator(
        task_id='latest_only',
        python_callable=_latest_only
    )

    start >> print_context >> pick_erp_system >> [fetch_new_sales, read_data]
    start >> fetch_wheather >> clean_wheather  
    fetch_new_sales >> clean_new_sales
    read_data >> transform_data_  
    [clean_new_sales, transform_data_] >> join_erp_branch
    clean_wheather >> join_datasets
    join_erp_branch >> join_datasets >> train_model >> latest_only >> deploy_model
