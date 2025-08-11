import unittest
from pymongo import MongoClient
from app.utils.agent_service import call_ven_agent_service_rerun, TARGET_AGENT_IDS

# Mock task class
class MockTask:
    def __init__(self, task_id, input_data):
        self.task_id = task_id
        self.input_data = input_data

class TestCallVenAgentServiceRerun(unittest.TestCase):
    def setUp(self):
        # Connect to real MongoDB instance
        self.client = MongoClient("mongodb+srv://artifi:root@artifi.2vi2m.mongodb.net/?retryWrites=true&w=majority&appName=Artifi")
        self.task = MockTask(
            task_id="TEST123",
            input_data={
                "case_id": "PRO0001192",
                "modified_data": {
                    "sic_description": "test sic"
                }
            }
        )

    def test_call_ven_agent_service_rerun(self):
        # Call the function
        result = call_ven_agent_service_rerun(self.task)

        # Print the agent endpoint results
        print("Agent Endpoint Results:")
        for agent_name, output in result["outputData"]["agent_output"].items():
            print(f"Agent: {agent_name}")
            print(f"Output: {output}")
            print("-" * 50)

if __name__ == '__main__':
    unittest.main()