from typing import Dict, Any

from oar.kao.platform import Platform
from oar.kao.simsim import JobSimu, ResourceSetSimu
from oar.lib.globals import get_logger, init_oar
from oar.lib.hierarchy import Hierarchy
from oar.lib.resource import ResourceSet
from procset import ProcSet

logger = get_logger("oar.kamelot_basic")


class PlatformAdapter(Platform):
    def __init__(self, platform_data: Dict[str, Any]):
        super().__init__()
        self.platform_rs = platform_data
        self.platform_config_rs = platform_data['platform_config']
        self.scheduled_jobs_rs = platform_data['scheduled_jobs']
        self.waiting_jobs_rs = platform_data['waiting_jobs']

        # Print attributes
        # print(f"PlatformAdapter initialized with config: {self._config}")
        # print(f"Scheduled jobs: {self._scheduled_jobs}")
        # print(f"Waiting jobs: {self._waiting_jobs}")

        # Compute resource set structure
        data = self.platform_config_rs['resource_set']
        begin = list(data['default_intervals'].intervals())[0].inf
        end = list(data['default_intervals'].intervals())[0].sup
        hy = data['hierarchy']['partitions']
        if 'unit_partition' in data['hierarchy']:
            hy[data['hierarchy']['unit_partition']] = [
                ProcSet(i, i) for i in range(begin, end+1)
            ]
        available_upto = {}
        for (t, procset) in data['available_upto']:
            available_upto[t] = procset
        self.resource_set_py = ResourceSetSimu(
            roid_itvs=data['default_intervals'],
            available_upto=available_upto,
            hierarchy=hy
        )

        # Compute jobs structure
        waiting_jobs = {}
        for job in self.waiting_jobs_rs:
            waiting_jobs[job['id']] = self.rs_job_to_py(job)
        self.waiting_jobs = waiting_jobs, [job['id'] for job in self.waiting_jobs_rs], len(self.waiting_jobs_rs)

        # Compute scheduled jobs structure (not available for now)
        self.scheduled_jobs = {}

        # Set placeholder for assigned jobs
        self.assigned_jobs = []

    def rs_job_to_py(self, j):
        return JobSimu(
            id=j['id'],
            state="Waiting",
            queue_name=j['queue'],
            name=j['name'],
            project=j['project'],
            user=j['user'],
            assign=False,
            types={},
            res_set=[],
            mld_res_rqts=[
                (
                    id,
                    m['walltime'],
                    [(req['level_nbs'], req['filter']) for req in m['requests']]
                ) for id, m in j['moldables']
            ],
            run_time=0,
            deps=[],
            key_cache={},
            ts=False,
            ph=0,
        )

    def resource_set(self, session=None, config=None):
        return self.resource_set_py

    def get_time(self):
        # In tests, scheduling time starts at 0
        return int(0)

    def get_waiting_jobs(self, queue, reservation="None", session=None):
        return self.waiting_jobs

    def get_scheduled_jobs(self, session, resoure_set, job_security_time, now):
        return self.scheduled_jobs

    def get_data_jobs(self, session, waiting_jobs, waiting_job_ids, resource_set, job_security_time):
        # Nothing to do, everything is taken care of in get_waiting_jobs.
        pass

    def save_assigns(self, session, waiting_jobs, resource_set):
        # for jid, j in waiting_jobs.items():
        # logger.info(f"Job {j.id} assigned to resources: {j.res_set}, walltime: {j.walltime}, start time: {j.start_time}")
        self.assigned_jobs = waiting_jobs.values()
        pass

    def scheduled_jobs_benchmark_report(self):
        """ Returns scheduled data about jobs that are scheduled"""
        jobs = []
        for j in self.assigned_jobs:
            if not hasattr(j, 'walltime') or not hasattr(j, 'start_time'):
                logger.warning(f"Job {j.id} does not have walltime or start_time attributes.")
                continue

            procset = []
            for res in list(j.res_set.intervals()):
                procset.append((res.inf, res.sup))

            jobs.append({
                'id': j.id,
                'quotas_hit_count': 0,
                'begin': j.start_time,
                'end': j.start_time + j.walltime - 1,
                'proc_set': procset,
                'moldable_index': j.moldable_id,
            })
        return jobs
