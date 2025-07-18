import time

from oar.kao.platform import Platform
from oar.lib.resource import ResourceSet
from oar.lib.hierarchy import Hierarchy
from typing import Dict, Any, List

class PlatformAdapter(Platform):
    def __init__(self, platform_data: Dict[str, Any]):
        super().__init__()
        self._platform_data = platform_data
        self._config = platform_data['platform_config']
        self._scheduled_jobs = platform_data['scheduled_jobs']
        self._waiting_jobs = platform_data['waiting_jobs']

        # Printing attribites
        print(f"PlatformAdapter initialized with config: {self._config}")
        print(f"Scheduled jobs: {self._scheduled_jobs}")
        print(f"Waiting jobs: {self._waiting_jobs}")

    def resource_set(self, session=None, config=None):
        resource_set_data = self._config['resource_set']

        # TODO: generate correct ResourceSet based on the platform data


        return ResourceSet(session, config)

    def get_time(self):
        return int(time.time())

    def get_waiting_jobs(self, queue, reservation="None", session=None):
        return self._waiting_jobs, [job['id'] for job in self._waiting_jobs], len(self._waiting_jobs)

    def get_scheduled_jobs(self, *args):
        return self._scheduled_jobs

    # Implement other required methods similarly
    def get_data_jobs(self, *args):
        return {}

    def save_assigns(self, *args):
        pass

    def get_sum_accounting_window(self, *args):
        return None

    def get_sum_accounting_by_project(self, *args):
        return None

    def get_sum_accounting_by_user(self, *args):
        return None

    # Simulation methods
    def resource_set_simu(self):
        return self.resource_set()

    def get_time_simu(self):
        return self.get_time()

    def get_waiting_jobs_simu(self, queue):
        return self.get_waiting_jobs(queue)

    def get_scheduled_jobs_simu(self, resource_set, job_security_time, now):
        return self.get_scheduled_jobs()

    def get_data_jobs_simu(self, *args):
        return self.get_data_jobs(*args)

    def save_assigns_simu(self, jobs, resource_set):
        self.save_assigns(jobs, resource_set)

    def save_assigns_simu_and_default(self, jobs, resource_set):
        self.save_assigns(jobs, resource_set)
