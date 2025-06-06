import unittest
import Solution as Solution


class AbstractTest(unittest.TestCase):
    # before each test, setUp is executed
    def setUp(self) -> None:
        # Drop tables if they exist to ensure clean start
        Solution.drop_tables()
        # Create new tables
        Solution.create_tables()

    # after each test, tearDown is executed
    def tearDown(self) -> None:
        Solution.drop_tables()
