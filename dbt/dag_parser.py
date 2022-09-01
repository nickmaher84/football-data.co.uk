import json
import logging
import os
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


class DbtDagParser:
    """
    A utility class that parses out a dbt project and creates the respective task groups
    :param dag: The Airflow DAG
    :param dbt_project_dir: The directory containing the dbt_project.yml
    :param dbt_profiles_dir: The directory containing the profiles.yml
    :param dbt_target: The dbt target profile (e.g. dev, prod)
    :param dbt_tag: Limit dbt models to this tag if specified.
    """

    def __init__(
        self,
        dag=None,
        dbt_project_dir=None,
        dbt_profiles_dir=None,
        dbt_target=None,
        dbt_tag=None,
    ):

        self.dag = dag
        self.dbt_project_dir = dbt_project_dir
        self.dbt_profiles_dir = dbt_profiles_dir
        self.dbt_target = dbt_target
        self.dbt_tag = dbt_tag

    def make_dbt_task(self, dbt_verb):
        """
        Returns a BashOperator task to run a dbt command.
        Args:
            dbt_verb: 'compile', 'seed', 'run' or 'test'
        """
        return BashOperator(
            task_id=f'dbt_{dbt_verb}',
            bash_command=(
                f'dbt {dbt_verb} --target {self.dbt_target} '
                f'--profiles-dir {self.dbt_profiles_dir} '
                f'--project-dir {self.dbt_project_dir}'
            ),
            env={
                'DBT_USER': '{{ conn.football_db.login }}',
                'DBT_ENV_SECRET_PASSWORD': '{{ conn.football_db.password }}',
                'DBT_HOST': '{{ conn.football_db.host }}',
                'DBT_SCHEMA': '{{ conn.football_db.schema }}',
                'DBT_PORT': '{{ conn.football_db.port }}',
            },
            dag=self.dag,
        )

    def make_dbt_model_task(self, dbt_verb, node_name, task_group):
        """
        Takes the manifest JSON content and returns a BashOperator task
        to run a dbt command.
        Args:
            dbt_verb: 'run' or 'test'
            node_name: The name of the node
            task_group: The TaskGroup the task should be added to
        Returns: A BashOperator task that runs the respective dbt command
        """

        model_name = node_name.split('.')[-1]
        if dbt_verb == 'test':
            node_name = node_name.replace('model', 'test')  # Just a cosmetic renaming of the task

        dbt_task = BashOperator(
            task_id=node_name,
            task_group=task_group,
            bash_command=(
                f'dbt {dbt_verb} --target {self.dbt_target} '
                f'--models {model_name} '
                f'--profiles-dir {self.dbt_profiles_dir} '
                f'--project-dir {self.dbt_project_dir}'
            ),
            env={
                'DBT_USER': '{{ conn.football_db.login }}',
                'DBT_ENV_SECRET_PASSWORD': '{{ conn.football_db.password }}',
                'DBT_HOST': '{{ conn.football_db.host }}',
                'DBT_SCHEMA': '{{ conn.football_db.schema }}',
                'DBT_PORT': '{{ conn.football_db.port }}',
            },
            pool_slots=4,
            dag=self.dag,
        )
        # Keeping the log output, it's convenient to see when testing the python code outside of Airflow
        logging.info('Created task: %s', node_name)
        return dbt_task

    def load_dbt_manifest(self):
        """
        Helper function to load the dbt manifest file.
        Returns: A JSON object containing the dbt manifest content.
        """
        manifest_path = os.path.join(self.dbt_project_dir, 'target/manifest.json')
        with open(manifest_path) as f:
            file_content = json.load(f)
        return file_content

    def make_dbt_task_group(self, dbt_verb):
        """
        Parse out a JSON file and populates a task group with dbt tasks
        Returns: TaskGroup
        """

        task_group = TaskGroup(group_id=f'dbt_{dbt_verb}')

        manifest_json = self.load_dbt_manifest()
        dbt_tasks = {}

        # Create the tasks for each model
        for node_name in manifest_json['nodes'].keys():
            if node_name.split('.')[0] == 'model':
                tags = manifest_json['nodes'][node_name]['tags']
                # Only use nodes with the right tag, if tag is specified
                if (self.dbt_tag and self.dbt_tag in tags) or not self.dbt_tag:
                    # Make the run nodes
                    dbt_tasks[node_name] = self.make_dbt_model_task('run', node_name, task_group)

            elif node_name.split('.')[0] == 'seed':
                tags = manifest_json['nodes'][node_name]['tags']
                # Only use nodes with the right tag, if tag is specified
                if (self.dbt_tag and self.dbt_tag in tags) or not self.dbt_tag:
                    # Make the run nodes
                    dbt_tasks[node_name] = self.make_dbt_model_task('seed', node_name, task_group)

        # Add upstream and downstream dependencies for each run task
        for node_name in manifest_json['nodes'].keys():
            if node_name.split('.')[0] == 'model':
                tags = manifest_json['nodes'][node_name]['tags']
                # Only use nodes with the right tag, if tag is specified
                if (self.dbt_tag and self.dbt_tag in tags) or not self.dbt_tag:
                    for upstream_node in manifest_json['nodes'][node_name]['depends_on']['nodes']:
                        upstream_node_type = upstream_node.split('.')[0]
                        if upstream_node_type in ('model', 'seed'):
                            dbt_tasks[upstream_node] >> dbt_tasks[node_name]

        return task_group

    def get_dbt_compile(self):
        """
        Getter method to retrieve the previously constructed dbt tasks.
        Returns: An Airflow task with dbt compile node.
        """
        return self.make_dbt_task('compile')

    def get_dbt_seed(self):
        """
        Getter method to retrieve the previously constructed dbt tasks.
        Returns: An Airflow task with dbt compile node.
        """
        return self.make_dbt_task('seed')

    def get_dbt_run(self, expanded=False):
        """
        Getter method to retrieve the previously constructed dbt tasks.
        Returns: An Airflow task group with dbt run nodes.
        """
        if expanded:
            return self.make_dbt_task_group('run')

        else:
            return self.make_dbt_task('run')

    def get_dbt_test(self, expanded=False):
        """
        Getter method to retrieve the previously constructed dbt tasks.
        Returns: An Airflow task group with dbt test nodes.
        """
        if expanded:
            return self.make_dbt_task_group('test')

        else:
            return self.make_dbt_task('test')
