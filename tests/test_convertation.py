import shutil
import unittest
import ConvertStorage

import os
import json
import subprocess
import git
import logging
from sys import platform


global test_cfg
global conf
global logger


def start_logger(conf):
    global logger
    logger = logging.getLogger(__name__)


def create_empty_db():
    global logger
    global conf
    global test_cfg

    onec = conf['onec']
    info_base = conf['info_base']

    command_line = '"{start_path}" CREATEINFOBASE {connection_string} /DisableStartupDialogs ' \
                   '/AddToList  "{list_base_name}" /L ru /VL ru  ' \
                   '/Out "{log_path}" ' \
                   ' '.format(start_path=onec['start_path'],
                              connection_string=info_base['connection_string'],
                              list_base_name=test_cfg['base_name'],
                              log_path=onec['log_file_path'])

    logger.info("Начало создания тестовой базы; %s", command_line)
    subprocess.run(command_line, shell=False, timeout=test_cfg['bd_creating_timeout'])
    oc_msg = ConvertStorage.read_oc_log(conf)
    logger.info(oc_msg)
    logger.info("Завершено создание тестовой базы")


def load_empty_db():
    global logger
    global conf
    global test_cfg

    onec = conf['onec']
    info_base = conf['info_base']

    command_line = '"{start_path}" DESIGNER /DisableStartupDialogs ' \
                        '/L ru /VL ru /IBConnectionString "{connection_string}" ' \
                        '/Out "{log_path}" /DumpResult "{result_path}"' \
                        ' '.format(start_path=onec['start_path'],
                                   # 1C требует двойных кавычек внутри строки
                                   connection_string=info_base['connection_string'].replace('"', '""'),
                                   log_path=onec['log_file_path'],
                                   result_path=onec['result_dump_path'])

    # т.к. загружаемая конфигурация не сохраняется в базу данных
    # достаточно вернуться к конфигурации базы данных
    # load_params = '/RollbackCfg'
    # второй вариант загрузка преднастроенной конфигурации из файла
    load_params = '/RestoreIB "{}"'.format(test_cfg['empty_dt_path'])
    load_command = command_line + ' ' + load_params

    logger.info(f'Начало загрузки архива тестовой базы. {load_command}')
    subprocess.run(load_command, shell=False, timeout=test_cfg['bd_creating_timeout'])
    oc_msg = ConvertStorage.read_oc_log(conf)
    if oc_msg != 'Загрузка информационной базы успешно завершена':
        raise ValueError(f'Ошибка восстановления пустой базы. {oc_msg}')

    logger.info(oc_msg)
    logger.info('Завершена загрузка архива тестовой базы.')


# костыль, для удаления папки git т.к.
# при удалении командами скрипта
# выдается ошибка прав доступа к файлам
def del_git_folder(src_path):
    """rmdir /S /Q C:\projects\StorageToGit\tests\test data\src"""
    command_line = f'rmdir /S /Q "{src_path}"'
    subprocess.run(command_line, shell=True)
    pass


def remove_test_data_folder():
    global logger
    global conf
    global test_cfg

    data_path = test_cfg['data_path']
    git_cfg = conf['git']
    git_path = git_cfg['path']  # путь к папке репозитория

    # work_path путь к папке содержащей репозиторий
    # Эта папка не обязательно совпадает с папкой исходников git_cfg['configuration_src_path']
    work_path = os.path.split(git_path)[0]

    # удаление папок содержащих гит-репо
    # вынесено отдельно из-за ошибок с правами
    # на удаление файлов средствами python
    if os.path.exists(work_path):
        if platform == 'win64' or platform == 'win32':
            del_git_folder(work_path)
        else:
            shutil.rmtree(work_path)

    bare_path = test_cfg['git_bare_path']
    if os.path.exists(bare_path):
        if platform == 'win64' or platform == 'win32':
            del_git_folder(bare_path)
        else:
            shutil.rmtree(bare_path)
    # завершено удаление тестовых репо

    # удаляем оставшиеся тестовые данные
    if os.path.exists(data_path):
        shutil.rmtree(data_path)

    pass


def init_test_data_folder():
    global logger
    global conf
    global test_cfg

    data_path = test_cfg['data_path']
    os.makedirs(data_path)

    git_cfg = conf['git']
    git_path = git_cfg['path']  # путь к папке репозитория
    # work_path путь к папке содержащей репозиторий
    # Эта папка не обязательно совпадает с папкой исходников git_cfg['configuration_src_path']
    work_path = os.path.split(git_path)[0]
    if not os.path.exists(work_path):
        os.makedirs(work_path)

    # создаем тестовые гит репо.
    # центральный для push
    # и локальный для commit
    bare_path = test_cfg['git_bare_path']
    if not os.path.exists(bare_path):
        os.makedirs(bare_path)
    git.Repo.init(bare_path, bare=True)
    git_env = git.cmd.Git(work_path)
    git_command = f'git clone "{bare_path}"'
    git_env.execute(command=git_command)

    # т.к. путь к исходникам может не совпадать
    # с путем к репо, то создаем папку исходников
    # при необходимости
    src_path = git_cfg['configuration_src_path']
    if not os.path.exists(src_path):
        os.makedirs(src_path)

    # инициализируем тестовый гит
    init_file_path = os.path.join(git_path, 'init_file.txt')
    with open(init_file_path, mode='w') as init_file:
        init_file.write('init repo')
    git_env = git.cmd.Git(git_path)
    git_env.execute(command='git add .')
    git_env.execute(command='git commit -m "init commit"')
    git_env.execute(command='git push')

    pass


def create_test_db():
    create_empty_db()
    load_empty_db()
    pass


def setUpModule():
    global logger
    global conf
    global test_cfg

    conf_path = os.path.abspath("C:\\projects\\StorageToGit\\tests\\config.json")
    with open(conf_path, mode="r", encoding="utf-8") as conf_file:
        test_cfg = json.load(conf_file)

    with open(test_cfg['script_config_path'], mode='r', encoding="utf-8") as conf_file:
        conf = json.load(conf_file)

    remove_test_data_folder()
    init_test_data_folder()
    ConvertStorage.start_logger(conf)
    logger = ConvertStorage.logger
    # start_logger(conf)

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
