from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
import requests
import json
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('github_api_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
# Check if the Github API is available
    def is_api_available():
        response = requests.get('https://api.github.com')
        if response.status_code == 200:
            print('Github API is available')
            # Print the response in json format
            print(response.json())
            
        else:
            print('Github API is not available')

    is_api_available = PythonOperator(
        task_id='is_api_available',
        python_callable=is_api_available
    )

 # Get a list of Repos
    def _get_repos():
        url = 'https://api.github.com/users/airbnb/repos'
        response = requests.get(url)
        repos = json.loads(response.text)
        return repos
    
    get_repos = PythonOperator(
        task_id='get_repos',
        python_callable=_get_repos
    )

# From the list of Repos, get a list of open PRs
    def _get_open_prs(**context):
        repos = context['task_instance'].xcom_pull(task_ids='get_repos')
        open_prs = []
        for repo in repos:
            pulls_url = repo['pulls_url'].split('{')[0]
            response = requests.get(pulls_url)
            prs = json.loads(response.text)
            for pr in prs:
                if pr['state'] == 'open':
                    open_prs.append(pr)
        return open_prs
    
    get_open_prs = PythonOperator(
        task_id='get_open_prs',
        python_callable=_get_open_prs
    )

    # For each open PR, update the PR title to append "- Work in Progress" and Summary to append "Your PR is a work in progress. Please do not merge."
    def _update_prs(**context):
        open_prs = context['task_instance'].xcom_pull(task_ids='get_open_prs')
        for pr in open_prs:
            pr_url = pr['url']
            pr_title = pr['title']
            pr_summary = pr['body']
            pr_title = pr_title + ' - Work in Progress'
            pr_summary = pr_summary + '\n\nYour PR is a work in progress. Please do not merge.'
            payload = {'title': pr_title, 'body': pr_summary}
            response = requests.patch(pr_url, json.dumps(payload), auth=('airflow', os.environ['GITHUB_ACCESS_TOKEN']))
            print(response.text)

    update_prs = PythonOperator(
        task_id='update_prs',
        python_callable=_update_prs
    )

    # Print out the updated PR titles and PR summaries
    def _print_prs(**context):
        open_prs = context['task_instance'].xcom_pull(task_ids='get_open_prs')
        for pr in open_prs:
            print(pr['title'])
            print(pr['body'])
            print('-------------------')
            
    print_prs = PythonOperator(
        task_id='print_prs',
        python_callable=_print_prs
    )

    # Make a flow
    is_api_available >> get_repos >> get_open_prs >> update_prs >> print_prs