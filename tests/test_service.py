import unittest
from app.contracts import Request
from app.service import HazardResponseService

class ServiceTest(unittest.TestCase):
    def test_idempotent_open(self):
        service = HazardResponseService()
        request = Request("operator", "open", {"key": "sample"}, "r-1")
        first = service.handle(request)
        second = service.handle(request)
        self.assertEqual(first, second)
        self.assertEqual(first.state, "open")

    def test_invalid_transition(self):
        service = HazardResponseService()
        result = service.handle(Request("operator", "close", {"key": "sample"}, "r-2"))
        self.assertFalse(result.accepted)

if __name__ == "__main__":
    unittest.main()
