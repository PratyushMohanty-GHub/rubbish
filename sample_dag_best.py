# Import the required libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
import requests
import json

from github import Github
from github import Auth

# # Login authentication
# user_login = ""
# user_password = ""
# auth = Auth(user_login, user_password)

# OAuth token authentication
access_token = "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"
auth = Auth.Token(access_token)

# Create a Github instance
g = Github(auth=auth)

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    # Retry in 15 seconds if the task fails
    'retry_delay': timedelta(seconds=15)
}

# Instantiate the DAG object
with DAG('github_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    # CHeck if the Github API is available
    def _is_api_available():
        response = requests.get('https://github.com/PratyushMohanty-GHub')
        if response.status_code == 200:
            print('Github API is available')
        else:
            print('Github API is not available')
    
    is_api_available = PythonOperator(
        task_id='is_api_available',
        python_callable=_is_api_available
    )

    repository_name = 'PratyushMohanty-GHub/rubbish'
    # Check if the repository exists
    def _is_repo_available():
        repo = g.get_repo(repository_name)
        if repo:
            print('Repository {} is available'.format(repository_name))
        else:
            print('Repository {} does not exist'.format(repository_name))
    
    is_repo_available = PythonOperator(
        task_id='is_repo_available',
        python_callable=_is_repo_available
    )

    # Get the list of open PRs from the repository
    def _get_open_prs():
        repo = g.get_repo(repository_name)
        open_prs = repo.get_pulls(state='open', sort='created')
        print('There are {} open PRs in repository {}'.format(open_prs.totalCount, repository_name))

        # Make a key value pair of PRid:  PR title and body
        prs = {}
        for pr in open_prs:
            prs[pr.number] = {'title': pr.title, 'body': pr.body}
            # Modify the title and body of the PR
            pr.edit(title='[Automated by airflow]' + pr.title, body='[Automated by airflow]' + pr.body)
        return prs

    
    get_open_prs = PythonOperator(
        task_id='get_open_prs',
        python_callable=_get_open_prs
    )

    # Modify the title and body of each open PR
    def _modify_prs(**context):
        print(context)
        repo = g.get_repo(repository_name)
        prs = context['task_instance'].xcom_pull(task_ids='get_open_prs')
        print(prs)
        print("END")

        # Add a prefix to the title and body of each PR
        for pr_id, pr in prs.items():
            print(pr_id, pr)
            print(pr['title'])
            print(pr['body'])

            # Modify the title and body of the PR
            pr_obj = repo.get_pull(pr_id)
            print("REached here")
            print(pr_obj)
            pr_obj.edit(title='[Automated by airflow]' + pr['title'], body='[Automated by airflow]' + pr['body'])

            print('Modified PR {}'.format(pr_id))

    modify_prs = PythonOperator(
        task_id='modify_prs',
        python_callable=_modify_prs
    )

    is_api_available >> is_repo_available >> get_open_prs >> modify_prs