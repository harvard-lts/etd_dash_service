import tasks.tasks as tasks

FEATURE_FLAGS = "feature_flags"


class TestTasksClass():

    def test_send_to_dash(self):
        message = {"job_ticket_id": "test_ticket_id", "unit_test": "true",
                   FEATURE_FLAGS: {
                    'dash_feature_flag': "off",
                    'alma_feature_flag': "off",
                    'send_to_drs_feature_flag': "off",
                    'drs_holding_record_feature_flag': "off"},
                   "identifier": "30522803"}
        retval = tasks.send_to_dash(message)
        assert "job_ticket_id" in retval
        assert "feature_flags" in retval
        assert "identifier" in retval
        assert retval["identifier"] == "30522803"
