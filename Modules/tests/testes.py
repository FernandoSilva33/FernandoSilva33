import unittest
from unittest.mock import patch
from datetime import datetime
import os

class TestCode(unittest.TestCase):
    def setUp(self):
        self.pasta_documentos = os.path.expanduser("~" + os.sep + "Documents")
        self.pasta_automato = os.path.join(self.pasta_documentos, "Automato")
        self.log_dir = os.path.join(self.pasta_automato, "Logs")
        self.hora_carga = '05:30'
        self.time_try = 30

    def tearDown(self):
        pass

    def test_log_message_writes_to_file(self):
        message = "Test message"
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        expected_file_path = os.path.join(self.log_dir, "cargaLog_{}.txt".format(now.strftime("%Y.%m.%d_%H%M%S")))

        with patch('builtins.open', create=True) as mock_file:
            log_message(message)
            mock_file.assert_called_once_with(expected_file_path, 'a')
            mock_file.return_value.write.assert_called_once_with("[{}] {}\n".format(timestamp, message))

    def test_restart_hora_returns_correct_value(self):
        expected_hora_carga = '05:30'
        self.assertEqual(restart_hora(), expected_hora_carga)

if __name__ == '__main__':
    unittest.main()