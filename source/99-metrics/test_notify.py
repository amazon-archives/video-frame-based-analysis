from __future__ import unicode_literals
from source.lib.notify import Notify
from source.lib.logger import Logger
from uuid import uuid4
from decimal import Decimal
from unittest import TestCase
from unittest import TestLoader
from unittest import TextTestRunner

# Instantiate Class
log_level = 'info'
logger = Logger(loglevel=log_level)
notify = Notify(logger)

class NotifyTest(TestCase):

    def test_backend_metrics(self):

        uuid = str(uuid4())
        solution_id = 'SO_unit_test'
        customer_uuid = uuid
        logger.info("UUID: " + customer_uuid)
        data = {'key_string1': '2017-7-6',
                 'key_string2': '12345',
                 'decimal': Decimal('1')
                 }
        url = 'https://oszclq8tyh.execute-api.us-east-1.amazonaws.com/prod/generic'
        response = notify.metrics(solution_id, customer_uuid, data, url)
        self.assertTrue(response == 200)

if __name__ == '__main__' and __package__ is None:
    suite = TestLoader().loadTestsFromTestCase(NotifyTest)
    TextTestRunner(verbosity=2).run(suite)
