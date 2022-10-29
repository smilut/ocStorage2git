import argparse
import os
import json
import subprocess
import sys

import git
import logging

from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import multiprocessing
from multiprocessing import Process
from time import sleep


class OCcommand:
    """Структура описания параметров команды 1С"""
    command_line: str
    time_out: int
    desc: str
    successful_msg: str


# организуем параллельность загрузки конфигурации
# в основную конфигурацию информационной базы
# и выполнение git add, commit, push
lock = multiprocessing.Lock()

# логирование в параллельных процессах

def curr_logger_id():
    pid = os.getpid()
    return f'{__name__}_{pid}'


def main_logger_config(conf: dict):
    log_cfg = conf['logging']
    log_path: str = log_cfg['path']

    rotate_time = log_cfg['rotate_time']
    rotate_interval = log_cfg['rotate_interval']
    if rotate_time == 'midnight':
        handler = TimedRotatingFileHandler(log_path, when=rotate_time, backupCount=log_cfg['copy_count'],
                                           encoding='utf-8')
    else:
        handler = TimedRotatingFileHandler(log_path, when=rotate_time, interval=rotate_interval,
                                           backupCount=log_cfg['copy_count'],
                                           encoding='utf-8')

    handler.setFormatter(logging.Formatter('%(asctime)s; %(levelname)s; %(name)s; %(message)s; %(desc)s',
                                           defaults={"desc": ''}))

    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName(log_cfg['level']))
    logger.addHandler(handler)


def main_log_listener(conf: dict, queue: multiprocessing.Queue, log_listener_on: multiprocessing.Queue):
    main_logger_config(conf)
    while log_listener_on.empty() or not(queue.empty()): # True:
        while not queue.empty():
            record = queue.get()
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        sleep(0.1)


def subprocess_logger_config(conf: dict, queue: multiprocessing.Queue):
    logger_id = curr_logger_id()
    log_cfg = conf['logging']
    handler = logging.handlers.QueueHandler(queue)
    logger = logging.getLogger(logger_id)
    logger.addHandler(handler)
    logger.setLevel(logging.getLevelName(log_cfg['level']))


def start_main_logger(conf: dict, queue: multiprocessing.Queue, log_listener_on: multiprocessing.Queue):
    listener = multiprocessing.Process(target=main_log_listener, args=(conf, queue, log_listener_on))
    listener.start()
    return listener

# завершение секции логирования


# блок обработки команд 1С
# функции данного модуля могут выполняться как в основном потоке
# так и в дочерних процессах
# для основного потока logger_id = 'main'

# формирует общую часть командной строки запуска 1С
# отвечает за подключение к информационной базе
def get_onec_command_line(conf, start_type: str) -> str:
    onec = conf['onec']
    info_base = conf['info_base']
    user = ''
    if info_base['windows_auth']:
        wa = ''
        password = ''
    else:
        wa = '/WA-'
        if info_base['user'] == '':
            raise ValueError('Не указано имя пользователя')
        else:
            user = '/N{}'.format(info_base['user'])

        password = ''
        if info_base['password'] != '':
            password = '/P{}'.format(info_base['password'])

    onec_command_line = '{start_path} {start_type} {wa_flag} /DisableStartupDialogs {user_name} ' \
                        '{passwd} /L ru /VL ru /IBConnectionString "{connection_string}" ' \
                        '/Out "{log_path}" /DumpResult "{result_path}"' \
                        ' '.format(start_path=onec['start_path'],
                                   start_type=start_type,
                                   wa_flag=wa,
                                   user_name=user,
                                   passwd=password,
                                   # 1C требует двойных кавычек внутри строки
                                   connection_string=info_base['connection_string'].replace('"', '""'),
                                   log_path=onec['log_file_path'],
                                   result_path=onec['result_dump_path'])
    return onec_command_line


# общая функция чтения лог файла 1С
# при выполнениее команды 1С могут быть сформированы два
# лог файла out.txt и result.txt
# имена файлов задаются ключами при запуске
def read_oc_log_file(log_path: str):
    logger = logging.getLogger(curr_logger_id())
    log_data = ''
    try:
        if os.path.exists(log_path):
            with open(log_path, 'r', encoding="utf_8_sig") as oc_log:
                log_data = oc_log.read().rstrip()
            try:
                os.remove(log_path)
            except Exception:
                logger.exception("Ошибка удаления лога 1С")
    except Exception:
        log_data = "Ошибка чтения лога 1С."
        logger.exception(log_data)

    return log_data


# Читает лог выполнения операции при запуске 1С
# в пакетном режиме
# Проблема в том, что данный файл формируется не всегда.
def read_oc_log(conf: dict) -> str:
    log_path = conf['onec']['log_file_path']
    oc_msg = read_oc_log_file(log_path)
    return oc_msg


# Читает результат выполнения операции при запуске 1С
# в пакетном режиме. В файле либо 0 - успех
# либо 1 - ошибка.
# Проблема в том, что данный файл формируется не всегда.
def read_oc_result(conf: dict) -> int:
    log_path = conf['onec']['result_dump_path']
    oc_result = read_oc_log_file(log_path)
    if oc_result != '1':
        return 0
    else:
        return 1


def execute_command(conf: dict, oc_command: OCcommand):
    logger = logging.getLogger(curr_logger_id())
    logger.info(f'Начало: {oc_command.desc}')
    logger.info("Команда: %s", oc_command.command_line)
    subprocess.run(oc_command.command_line, shell=False, timeout=oc_command.time_out)
    oc_msg = read_oc_log(conf)
    oc_res = read_oc_result(conf)
    logger.info(f'Сообщение 1С: {oc_msg}')
    logger.info(f'Завершено: {oc_command.desc}')
    if oc_res != 0 or oc_msg != oc_command.successful_msg:
        err_desc = f'Выполненение:{oc_command.desc}; команда:{oc_command.command_line}, завершено с ошибкой '
        raise ValueError(err_desc)

# завершение блока обработки команд 1С


# подготовка данных:
# первичная очистка основной конфигурации,
# определение номера версии для выгрузки из хранилища
# и т.д.
# функции данного блока выполняются в потоке основного процесса

# чтение аргументов командной строки
def init_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--conf", help="set path to config file", type=str, default="")
    args = parser.parse_args()

    return args


# получение пути к файлу config.json
def get_conf_path() -> str:
    args = init_args()
    conf_path = args.conf
    if conf_path == "":
        conf_path = os.path.join(os.getcwd(), "config.json")

    return conf_path


# получение пути файла с номером обработанной конфигурации
def get_storage_data_path(conf) -> str:
    return conf['storage']['version_path']


# чтение настроек из файла
def init_configuration() -> dict:
    conf_path = get_conf_path()

    with open(conf_path, mode="r", encoding="utf-8") as conf_file:
        conf = json.load(conf_file)

    return conf


# приводит базу даных в исходное состояние перед
# запуском скрипта. Т.к. если основная конфигурация
# не соответсвует конфигурации базы данных запрос
# на продолжение блокирует генерацию истории хранилища
# из отчета по хранилищу
def restore_bd_configuration(conf: dict):
    command_line = get_onec_command_line(conf, 'DESIGNER')
    restore_params = ' /RollbackCfg'

    oc_command = OCcommand()
    oc_command.command_line = command_line + restore_params
    oc_command.desc = 'Восстановление конфигурации'
    oc_command.time_out = conf['onec']['timeout']
    oc_command.successful_msg = 'Возврат к конфигурации БД успешно завершен'

    execute_command(conf, oc_command)


# получает номер последней версии, которую удалось
# прочитать из хранилища.
# продолжать чтение надо с версии последняя+1
def get_last_storage_version(conf: dict) -> int:
    logger = logging.getLogger(curr_logger_id())
    storage_data_path = get_storage_data_path(conf)
    if os.path.exists(storage_data_path):
        with open(storage_data_path, mode='r') as storage_data_file:
            storage_data = json.load(storage_data_file)
            last_version = storage_data['last_version']
    else:
        last_version = 0

    logger.info("Прочитан номер версии прошлой выгрузки; %s", last_version)
    return last_version


# Команда формирования отчета по хранилищу конфигурации
# example:
# "c:\Program Files\1cv8\8.3.18.1289\bin\1cv8.exe" DESIGNER /WA-
# /DisableStartupDialogs /L ru /VL ru
# /IBConnectionString "File=""C:\Documents\StorageReceiver"";"
# /DumpResult "C:\Documents\1C\result.txt"
# /Out "C:\Documents\1C\log.txt" -NoTruncate
# /ConfigurationRepositoryF "C:\Storage\TestStorage"
# /ConfigurationRepositoryN ReadOnly
# /ConfigurationRepositoryReport “C:\Documents\1C\prj\reports\storage_report.mxl”
# -NBegin 1
def create_storage_report_command(conf: dict, last_version: int) -> OCcommand:
    onec = conf['onec']
    storage = conf['storage']
    start_version = last_version + 1
    command_line = get_onec_command_line(conf, 'DESIGNER')

    if storage['password'] == "":
        passwd_flag = ""
    else:
        passwd_flag = storage['password']

    report_param_str = '/ConfigurationRepositoryF "{storage_path}" ' \
                       '/ConfigurationRepositoryN {storage_user} {storage_passwd_flag} ' \
                       '/ConfigurationRepositoryReport "{report_path}" -NBegin {ver_num} ' \
                       ' '.format(storage_path=storage['path'],
                                  storage_user=storage['user'],
                                  storage_passwd_flag=passwd_flag,
                                  report_path=storage['report_path'],
                                  ver_num=start_version)

    oc_command = OCcommand()
    oc_command.command_line = command_line + ' ' + report_param_str
    oc_command.desc = 'Формирование отчета по хранилищу'
    oc_command.time_out = onec['timeout']
    oc_command.successful_msg = 'Отчет успешно построен'
    return oc_command


def create_storage_report(conf: dict, last_version: int):
    storage = conf['storage']

    # удаляем отчет от предыдущего запуска
    if os.path.exists(storage['report_path']):
        os.remove(storage['report_path'])

    oc_command = create_storage_report_command(conf, last_version)
    execute_command(conf, oc_command)


# Команда запуска обработки преобразования отчета по хранилиу
# конфигурации в json
# example:
# "c:\Program Files\1cv8\8.3.18.1289\bin\1cv8.exe" ENTERPRISE /WA- /DisableStartupDialogs
# /NСервисРаботыСХранилищем /P…..  /L ru /VL ru
# /F "C:\Users\milut\Documents\StorageReceiver"
# /Execute "C:\1C\Работа с хранилищем\ОтчетПоХранилищуВjson.epf"
# /C "C:\1C\Работа с хранилищем\storage_report003.mxl;C:\1C\Работа с хранилищем\storage_history.json"
# /Out "C:\Users\milut\Documents\1C\log.txt" -NoTruncate
def create_storage_history_command(conf: dict) -> OCcommand:
    command_line = get_onec_command_line(conf, 'ENTERPRISE')
    onec = conf['onec']
    storage = conf['storage']

    args_for_processor = '""{report_path}"";""{history_path}""' \
        .format(report_path=storage['report_path'],
                history_path=storage['json_report_path'])

    convert_param_str = '/Execute "{converter_path}" ' \
                        '/C "{args}" ' \
                        ' '.format(converter_path=onec['report_convert_processor_path'],
                                   args=args_for_processor)

    oc_command = OCcommand()
    oc_command.command_line = command_line + ' ' + convert_param_str
    oc_command.desc = 'Формирование истории хранилища'
    oc_command.time_out = onec['timeout']
    oc_command.successful_msg = ''

    return oc_command


def create_storage_history(conf: dict):
    storage = conf['storage']

    # удаляем историю оставшуюся от предыдущего запуска
    if os.path.exists(storage['json_report_path']):
        os.remove(storage['json_report_path'])

    oc_command = create_storage_history_command(conf)
    execute_command(conf, oc_command)


# преобразует json файл с историей хранилища
# в упорядоченный список структур, которые описывают версии
# хранилища. Далее по данному списку выполняется выгрузка
# истории хранилища в git
def read_storage_history(conf: dict) -> dict:
    logger = logging.getLogger(curr_logger_id())
    logger.info('Начало чтения файла истории хранилища')
    # корректируем строку пути, т.к. для 1С нужны кавычки, а для
    # python они вызывают ошибку
    history_path: str = (conf['storage']['json_report_path'])
    try:
        with open(history_path, 'r', encoding="utf_8_sig") as history_file:
            history_data = json.load(history_file)
    except Exception:
        logger.exception('Ошибка чтения файла истории хранилища; %s', history_path)
        raise
    logger.info('Завершено чтение файла истории хранилища')
    return history_data

# завершение блока подготовки данных


# блок выгрузки конфигурации в файлы

# команда обновления конфигурации до заданной версии хранилища
def update_to_storage_version_command(conf: dict, version_for_load: int) -> OCcommand:
    onec = conf['onec']
    storage = conf['storage']

    command_line = get_onec_command_line(conf, 'DESIGNER')

    if storage['password'] == "":
        passwd_flag = ""
    else:
        passwd_flag = storage['password']

    update_param_str = '/ConfigurationRepositoryF "{storage_path}" ' \
                       '/ConfigurationRepositoryN {storage_user} {storage_passwd_flag} ' \
                       '/ConfigurationRepositoryUpdateCfg -force -v {ver_num} ' \
                       ' '.format(storage_path=storage['path'],
                                  storage_user=storage['user'],
                                  storage_passwd_flag=passwd_flag,
                                  ver_num=version_for_load)

    oc_command = OCcommand()
    oc_command.command_line = command_line + ' ' + update_param_str
    oc_command.desc = 'Обновление из хранилища'
    oc_command.time_out = onec['update_timeout']
    oc_command.successful_msg = 'Обновление конфигурации из хранилища успешно завершено'

    return oc_command


# обновляет основную конфигурацию до указанной версии
# из хранилища. выполняется в основном потоке.
def update_to_storage_version(conf: dict, version_for_load: int):
    oc_command = update_to_storage_version_command(conf, version_for_load)
    execute_command(conf, oc_command)


# команда выгрузки кофигурации в файлы
def dump_configuration_to_git_command(conf: dict, first_dump: bool, ver: int) -> OCcommand:
    onec = conf['onec']
    git_options = conf['git']
    command_line = get_onec_command_line(conf, 'DESIGNER')

    dump_param_str = '/DumpConfigToFiles "{}"'.format(git_options['configuration_src_path'])

    oc_command = OCcommand()
    if first_dump:
        oc_command.command_line = command_line + ' ' + dump_param_str
    else:
        oc_command.command_line = command_line + ' ' + dump_param_str + ' -update'
    oc_command.desc = f'Выгрузка в git {ver}'
    oc_command.time_out = onec['dump_timeout']
    oc_command.successful_msg = ''

    return oc_command


# выгружает основную конфигурацию в локальную папку git
# выполняется в дочернем процессе
def dump_configuration_to_git(conf: dict, first_dump: bool, ver: int, lock: multiprocessing.Lock, queue: multiprocessing.Queue):
    lock.acquire()
    subprocess_logger_config(conf, queue)
    oc_command = dump_configuration_to_git_command(conf, first_dump, ver)
    execute_command(conf, oc_command)
    lock.release()

# завершение блока выгрузки конфигурации


# блок обработки команд git
# функции данного блока выполняются в дочерних процессах

# помещает все выгруженные в git изменения
# в remote git. выполняется как в основно, так и в дочернем потоках
def git_push(conf: dict):
    logger = logging.getLogger(curr_logger_id())
    logger.info('Начало git push')

    git_options = conf['git']
    repo = git.Repo(git_options['path'], search_parent_directories=False)
    # добавляем номер версии преред push
    # теоретически должно помочь при определении
    # новой порции кода в сонаре
    tag = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    repo.create_tag(tag)

    try:
        origin = repo.remotes['origin']
    except IndexError as ie:
        logger.exception("Ошибка получения удаленного репозитария")
        raise ie

    # for linux only
    # origin.push(kill_after_timeout=git_options['push_timeout'])
    origin.push()
    logger.info('Выполнение git push завершено')


# возвращает автора коммита для сохранения версии в git
def git_author_for_version(conf: dict, author: str) -> str:
    git_options = conf['git']
    storage = conf['storage']

    authors = storage['authors']
    default_mail = git_options['default_user_email']
    for val in authors:
        if author == val['user']:
            return '{author} <{mail}>'.format(author=author, mail=val['email'])

    return '{author} <{mail}>'.format(author=author, mail=default_mail)


# возвращает описание изменений версии для git commit
def get_commit_label(conf: dict, version_for_dump: int, version_data: dict) -> str:
    logger = logging.getLogger(curr_logger_id())
    git_opt = conf['git']
    ver_label = version_data['Version']
    comment = version_data['CommitMessage']
    changed_obj = 'Изменено:\n'
    for val in version_data['ChangedObjects']:
        changed_obj = changed_obj + ' ' + val + '\n'
    if len(changed_obj) > 512:
        changed_obj = changed_obj[:512] + '...'

    added_obj = 'Добавлено:\n'
    for val in version_data['AddedObjects']:
        added_obj = added_obj + ' ' + val + '\n'
    if len(added_obj) > 512:
        added_obj = added_obj[:512] + '...'

    commit_msg_prefix = git_opt['commit_msg_prefix']
    label = f'{commit_msg_prefix} ver:{version_for_dump}; {ver_label}; \n \n{comment}\n\n' \
            f'{added_obj} {changed_obj}\n'
    logger.info('Сообщение для git commit; %s', label)

    return label


# выполняет add, commit от имени пользователя поместившего версию в хранилище
# а также push в соответствии с настройками. выполняется в дочернем потоке
def git_commit_storage_version(conf: dict, version_for_dump: int, version_data: dict,
                               lock: multiprocessing.Lock, queue: multiprocessing.Queue):
    lock.acquire()
    subprocess_logger_config(conf, queue)
    logger = logging.getLogger(curr_logger_id())
    logger.info('Начало git add; %s', version_for_dump)
    git_options = conf['git']
    repo = git.Repo(git_options['path'], search_parent_directories=False)
    repo.index.add('*')
    logger.info('Завершен git add; %s', version_for_dump)

    ver_author = version_data['Author']
    git_author = git_author_for_version(conf, ver_author)
    label = get_commit_label(conf, version_for_dump, version_data)
    commit_stamp = datetime.strptime(version_data['CommitDate'] + ' ' + version_data['CommitTime'], "%d.%m.%Y %H:%M:%S")

    logger.info('Начало git commit %s', version_for_dump)
    repo.git.commit('-m', label, author=git_author, date=commit_stamp)
    logger.info('Завершен git commit; %s', version_for_dump)

    git_push(conf)

    save_last_version(conf, version_for_dump)
    logger.info('Завершена обработка версии %s', version_for_dump)

    lock.release()

# завершение блока команд git


# проходит по версиям хранилища от меньшей к большей
# и выгружает данные каждой версии из истории в git
def scan_history(conf: dict, queue: multiprocessing.Queue):
    # при каждом запуске скрипта промежуточная конфигурация возвращается
    # к конфе базы данных, поэтому выгружать в файлы надо всю загруженную
    # из хранилища конфигурацию
    first_dump = True
    logger = logging.getLogger(curr_logger_id())

    logger.info('Начало переноса истории хранилища в git')
    history_data = read_storage_history(conf)
    versions = list()
    for key in history_data.keys():
        versions.append(int(key))

    versions.sort()
    git_process = None
    for ver in versions:
        logger.info(f'Начало обработки версии {ver}')
        version_data = history_data[str(ver)]
        update_to_storage_version(conf, ver)  # загрузка из хранилища

        # выгрузка в локальную папку git
        dump_process = Process(target=dump_configuration_to_git, args=(conf, first_dump, ver, lock, queue))
        dump_process.start()
        dump_process.join()

        # add, commit and push изменений в локальном git
        git_process = Process(target=git_commit_storage_version, args=(conf, ver, version_data, lock, queue))
        git_process.start()

        # т.к. очередная версия хранилища уже загружена в основную конфигурацию,
        # то следующая выгрузка в гит может быть инкрементной
        first_dump = False

    if not (git_process is None):
        git_process.join()

    logger.info('Завершен перенос истории хранилища в git')

# сохраняет номер последней обработанной версии
# для того чтобы продолжить следующую загрузку
# со следующей
def save_last_version(conf: dict, last_version: int):
    logger = logging.getLogger(curr_logger_id())
    storage_data_path = get_storage_data_path(conf)
    with open(storage_data_path, mode='w') as storage_data_file:
        json.dump({'last_version': last_version}, storage_data_file)

    logger.info('Сохранен номер обработанной версии; %s', storage_data_path)


# основной скрипт. вынесен в отдельную функцию для удобства тестирования.
def convert_storage_to_git(conf):
    queue = multiprocessing.Queue(-1)
    log_listener_on = multiprocessing.Queue(-1)

    listener = start_main_logger(conf, queue, log_listener_on)
    subprocess_logger_config(conf, queue)
    logger = logging.getLogger(curr_logger_id())
    try:
        logger.info('Запуск скрипта')
        last_version = get_last_storage_version(conf)
        restore_bd_configuration(conf)
        create_storage_report(conf, last_version)
        create_storage_history(conf)
        scan_history(conf, queue)

        logger.debug('Завершение скрипта')
    except Exception as e:
        logger.exception('Script error')
        raise e
    finally:
        log_listener_on.put("Stop logging")
        listener.join()


if __name__ == '__main__':
    conf = init_configuration()
    convert_storage_to_git(conf)
    sys.exit()
