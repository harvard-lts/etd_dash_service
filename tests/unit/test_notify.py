from lib.notify import notify


class TestWorkerClass():

    def test_verify_job_monitor_connection_success(self):
        jobMonitor = 'https://jobmon-dev.lib.harvard.edu'
        n = notify("test")
        assert n.verify_job_monitor_connection(jobMonitor)

    def test_verify_job_monitor_connection_fail(self):
        jobMonitor = 'http://abcd.efg'
        n = notify("test")
        assert not n.verify_job_monitor_connection(jobMonitor)
