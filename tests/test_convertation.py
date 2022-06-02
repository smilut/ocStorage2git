import shutil
import unittest
import ConvertStorage

import os
import json
import subprocess


def load_empty_cf():
    onec = conf['onec']
    test_env = conf['test']
    command_line = ConvertStorage.get_onec_command_line(conf, 'DESIGNER')

    load_params = '/LoadCfg {} /UpdateDBCfg'.format(test_env['empty_cf_path'])
    load_command = command_line + ' ' + load_params

    subprocess.run(load_command, shell=False, timeout=onec['timeout'])


def clear_test_data():
    test_env = conf['test']
    data_path = test_env['data_path']
    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    os.makedirs(data_path)


def setUpModule():
    global conf

    print('Подготовка тестовой среды.\n')
    conf_path = os.path.abspath("C:\\projects\\StorageToGit\\tests\\config.json") #os.path.join(os.getcwd(), )
    with open(conf_path, mode="r", encoding="utf-8") as conf_file:
        conf = json.load(conf_file)

    clear_test_data()
    load_empty_cf()
    print('База для переноса истории хранилища в Git восстановлена.\n')


def tearDownModule():
    print("setUpModule: " + __name__ + " tear down")
    print()


class ConvertionTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("setUpClass: " + __name__ + " set up")
        pass

    @classmethod
    def tearDownClass(self):
        print("tearClass: " + __name__ + " tear down")
        pass

    def test_create_storage_report(self):
        report_path = conf['storage']['report_path']
        before = os.path.exists(report_path)
        assert (before == False)
        ConvertStorage.create_storage_report(conf, 0)
        after = os.path.exists(report_path)
        assert (after == True)
        os.remove(report_path)
        print("Завершен тест создания отчета:", before, after, '\n')

    def test_create_storage_history(self):
        report_path = conf['storage']['report_path']
        history_path = conf['storage']['json_report_path']
        ConvertStorage.create_storage_report(conf, 0)
        before = os.path.exists(history_path)
        assert (before == False)
        ConvertStorage.create_storage_history(conf)
        after = os.path.exists(history_path)
        assert (after == True)
        os.remove(report_path)
        os.remove(history_path)
        print("Завершен тест создания истории:", before, after, '\n')

    def test_read_storage_history(self):
        report_path = conf['storage']['report_path']
        history_path = conf['storage']['json_report_path']
        ConvertStorage.create_storage_report(conf, 0)
        ConvertStorage.create_storage_history(conf)
        history = ConvertStorage.read_storage_history(conf)
        assert isinstance(history, dict)
        os.remove(report_path)
        os.remove(history_path)
        print("Завершен тест чтения истории.\n")


if __name__ == '__main__':
    unittest.main()