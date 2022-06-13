import shutil
import unittest
import ConvertStorage

import os
import json
import subprocess
import git
import logging


global conf
global logger


def start_logger(conf):
    global logger
    logger = logging.getLogger(__name__)


def create_empty_db():
    global logger
    global conf

    onec = conf['onec']
    info_base = conf['info_base']
    test_cfg = conf['test']

    command_line = '{start_path} CREATEINFOBASE {connection_string} /DisableStartupDialogs ' \
                   '/AddToList  "{list_base_name}" /L ru /VL ru  ' \
                   '/Out {log_path} ' \
                   ' '.format(start_path=onec['start_path'],
                              connection_string=info_base['connection_string'],
                              list_base_name=test_cfg['base_name'],
                              log_path=onec['log_file_path'])

    logger.info("Начало создания тестовой базы; %s", command_line)
    subprocess.run(command_line, shell=False, timeout=test_cfg['bd_creating_timeout'])
    oc_msg = ConvertStorage.read_oc_log(conf)
    logger.info(oc_msg)
    logger.info("Завершено создание тестовой базы")


def load_empty_cf():
    onec = conf['onec']
    test_env = conf['test']
    info_base = conf['info_base']

    command_line = '{start_path} DESIGNER /DisableStartupDialogs ' \
                        '/L ru /VL ru /IBConnectionString {connection_string} ' \
                        '/Out {log_path} /DumpResult {result_path}' \
                        ' '.format(start_path=onec['start_path'],
                                   connection_string=info_base['connection_string'],
                                   log_path=onec['log_file_path'],
                                   result_path=onec['result_dump_path'])

    # т.к. загружаемая конфигурация не сохраняется в базу данных
    # достаточно вернуться к конфигурации базы данных
    # load_params = '/RollbackCfg'
    # второй вариант загрузка преднастроенной конфигурации из файла
    load_params = '/LoadCfg {} /UpdateDBCfg'.format(test_env['empty_cf_path'])
    load_command = command_line + ' ' + load_params

    logger.info(f'Начало загрузки конфигурации тестовой базы. {load_command}')
    subprocess.run(load_command, shell=False, timeout=test_env['bd_creating_timeout'])
    oc_msg = ConvertStorage.read_oc_log(conf)
    logger.info(oc_msg)
    logger.info(f'Завершена загрузка конфигурации тестовой базы.')


def load_empty_db():
    onec = conf['onec']
    test_env = conf['test']
    info_base = conf['info_base']

    command_line = '{start_path} DESIGNER /DisableStartupDialogs ' \
                        '/L ru /VL ru /IBConnectionString {connection_string} ' \
                        '/Out {log_path} /DumpResult {result_path}' \
                        ' '.format(start_path=onec['start_path'],
                                   connection_string=info_base['connection_string'],
                                   log_path=onec['log_file_path'],
                                   result_path=onec['result_dump_path'])

    # т.к. загружаемая конфигурация не сохраняется в базу данных
    # достаточно вернуться к конфигурации базы данных
    # load_params = '/RollbackCfg'
    # второй вариант загрузка преднастроенной конфигурации из файла
    load_params = '/RestoreIB {}'.format(test_env['empty_dt_path'])
    load_command = command_line + ' ' + load_params

    logger.info(f'Начало загрузки архива тестовой базы. {load_command}')
    subprocess.run(load_command, shell=False, timeout=test_env['bd_creating_timeout'])
    oc_msg = ConvertStorage.read_oc_log(conf)
    logger.info(oc_msg)
    logger.info('Завершена загрузка архива тестовой базы.')


def remove_test_data():
    global conf

    test_env = conf['test']
    data_path = test_env['data_path']
    git_cfg = conf['git']
    git_path = git_cfg['path']
    work_path = git_path[:len(git_path) - 4]
    src_path = git_cfg['configuration_src_path']

    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    if os.path.exists(git_path):
        shutil.rmtree(git_path)

    if os.path.exists(work_path):
        shutil.rmtree(work_path)

    if os.path.exists(src_path):
        shutil.rmtree(src_path)

    os.makedirs(data_path)
    os.makedirs(src_path)
    if not os.path.exists(work_path):
        os.makedirs(work_path)

    git.Repo.init(work_path)


def create_test_db():
    create_empty_db()
    #load_empty_cf()
    load_empty_db()
    pass


def setUpModule():
    global conf
    global logger

    conf_path = os.path.abspath("C:\\projects\\StorageToGit\\tests\\config.json")  # os.path.join(os.getcwd(), )
    with open(conf_path, mode="r", encoding="utf-8") as conf_file:
        conf = json.load(conf_file)

    remove_test_data()
    ConvertStorage.start_logger(conf)
    start_logger(conf)

    logger.info('Подготовка тестовой среды')
    create_test_db()
    logger.info('База для переноса истории хранилища в Git восстановлена')
    pass


def tearDownModule():
    pass


class ConvertionTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        pass

    @classmethod
    def tearDownClass(self):
        pass

    def test_000_clear_test_env(self):
        global conf
        global logger
        pass

    def test_010_create_storage_report(self):
        global conf
        global logger

        logger.info('Начало теста создания отчета')
        try:
            report_path = conf['storage']['report_path']
            before = os.path.exists(report_path)
            ConvertStorage.create_storage_report(conf, 0)
            after = os.path.exists(report_path)
            os.remove(report_path)
        except Exception as e:
            logger.exception('Ошибка формирования отчета')
            raise

        logger.info(f'Завершен тест создания отчета; {before} {after}')
        assert (after == True)

    def test_020_create_storage_history(self):
        global conf
        global logger

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
        logger.info(f'Завершен тест создания истории; {before} {after}')

    def test_030_read_storage_history(self):
        global conf
        global logger

        report_path = conf['storage']['report_path']
        history_path = conf['storage']['json_report_path']
        ConvertStorage.create_storage_report(conf, 0)
        ConvertStorage.create_storage_history(conf)
        history = ConvertStorage.read_storage_history(conf)
        assert isinstance(history, dict)
        os.remove(report_path)
        os.remove(history_path)
        logger.info('Завершен тест чтения истории')

    def test_040_read_storage_history(self):
        global conf
        global logger

        logger.info('Начало теста перноса истории в хранилище')
        try:
            ConvertStorage.convert_storage_to_git(conf)
        except Exception as e:
            logger.exception('Ошибка переноса истории хранилища')
            raise

        logger.info('Завершен тест переноса истории в хранилище')


if __name__ == '__main__':
    unittest.main()
