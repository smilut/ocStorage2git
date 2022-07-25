import argparse
import os
import sys
import json
import subprocess
import git
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

global logger
global first_dump


class OCcommand:
    """Структура описания параметров команды 1С"""
    command_line: str
    time_out: int
    desc: str
    successful_msg: str


def init_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--conf", help="set path to config file", type=str, default="")
    args = parser.parse_args()

    return args


def get_conf_path() -> str:
    args = init_args()
    conf_path = args.conf
    if conf_path == "":
        conf_path = os.path.join(os.getcwd(), "config.json")

    return conf_path


def init_configuration() -> dict:
    conf_path = get_conf_path()

    with open(conf_path, mode="r", encoding="utf-8") as conf_file:
        conf = json.load(conf_file)

    return conf


def start_logger(conf: dict):
    log_cfg = conf['logging']
    LOG_PATH: str = log_cfg['path']

    global logger

    rotate_time = log_cfg['rotate_time']
    rotate_interval = log_cfg['rotate_interval']
    if rotate_time == 'midnight':
        handler = TimedRotatingFileHandler(LOG_PATH, when=rotate_time, backupCount=log_cfg['copy_count'],
                                           encoding='utf-8')
    else:
        handler = TimedRotatingFileHandler(LOG_PATH, when=rotate_time, interval=rotate_interval,
                                           backupCount=log_cfg['copy_count'],
                                           encoding='utf-8')     

    handler.setFormatter(logging.Formatter('%(asctime)s; %(levelname)s; %(name)s; %(message)s; %(desc)s',
                                           defaults={"desc": ''}))

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(log_cfg['level']))
    logger.addHandler(handler)


def read_oc_log_file(log_path):
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


# приводит базу даных в исходное состояние перед
# запуском скрипта. Т.к. если основная конфигурация
# не соответсвует конфигурации базы данных запрос
# на продолжение блокирует генерацию истории хранилища
# из отчета по хранилищу
def restore_bd_configuration(conf):
    global logger
    global first_dump

    command_line = get_onec_command_line(conf, 'DESIGNER')
    restore_params = ' /RollbackCfg'

    oc_command = OCcommand()
    oc_command.command_line = command_line + restore_params
    oc_command.desc = 'Восстановление конфигурации'
    oc_command.time_out = conf['onec']['timeout']
    oc_command.successful_msg = 'Возврат к конфигурации БД успешно завершен'

    execute_command(conf, oc_command)
    first_dump = True


def get_storage_data_path(conf) -> str:
    return conf['storage']['version_path']


# получает номер последней версии, которую удалось
# прочитать из хранилища.
# продолжать чтение надо с версии последняя+1
def get_last_storage_version(conf) -> int:
    global logger

    storage_data_path = get_storage_data_path(conf)
    if os.path.exists(storage_data_path):
        with open(storage_data_path, mode='r') as storage_data_file:
            storage_data = json.load(storage_data_file)
            last_version = storage_data['last_version']
    else:
        last_version = 0

    logger.info("Прочитан номер версии прошлой выгрузки; %s", last_version)
    return last_version


# формирует общую часть командной строки запуска 1С
# отвечает за подключение к информационной базе
def get_onec_command_line(conf, start_type: str) -> str:
    onec = conf['onec']
    info_base = conf['info_base']
    if info_base['windows_auth']:
        wa = ''
        user = ''
        password = ''
    else:
        wa = '/WA-'

        user = ''
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

    command_line = get_onec_command_line(conf, 'DESIGNER')

    storage = conf['storage']
    if storage['password'] == "":
        passwd_flag = ""
    else:
        passwd_flag = storage['password']

    start_version = last_version + 1

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

    args_for_processor = '""{report_path}"";""{history_path}""'\
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


def terminate_script(conf: dict):
    global logger
    script_opt = conf['script']

    if script_opt['terminate']:
        terminate_time = datetime.strptime(script_opt['terminate_after'], "%H:%M").time()
        cur_time = datetime.now().time()
        if cur_time > terminate_time:
            logger.info('Выполнение скрипта остановлено по расписанию')
            if script_opt['push_after_convertation']:
                git_push(conf)
            else:
                logger.info('Выполнение git push при завершении скрипта отключено в файле настроек скрипта')

            sys.exit()


def git_push_after_time(conf: dict):
    global logger
    script_opt = conf['script']
    git_opt = conf['git']

    if not script_opt['push_after_convertation'] and git_opt['push_time'] != '':
        push_time = datetime.strptime(git_opt['push_time'], "%H:%M").time()
        cur_time = datetime.now().time()
        if cur_time > push_time:
            logger.info('git push по расписанию')
            git_push(conf)


def git_push(conf: dict):
    global logger

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

    pass

# проходит по версиям хранилища от меньшей к большей
# и выгружает данные каждой версии из истории в git
def scan_history(conf: dict):
    global first_dump

    # при каждом запуске скрипта промежуточная конфигурация возвращается
    # к конфе базы данных, поэтому выгружать в файлы надо всю загруженную
    # из хранилища конфигурацию
    first_dump = True
    logger.info('Начало переноса истории хранилища в git')
    history_data = read_storage_history(conf)
    versions = list()
    for key in history_data.keys():
        versions.append(int(key))

    versions.sort()
    for ver in versions:
        logger.info(f'Начало обработки версии {ver}')
        version_data = history_data[str(ver)]
        update_to_storage_version(conf, ver)
        dump_configuration_to_git(conf, ver, version_data)
        # т.к. очередная версия хранилища уже загружена в основную конфигурацию,
        # то следующая выгрузка в гит может быть инкрементной
        first_dump = False
        last_version = ver
        save_last_version(conf, last_version)
        logger.info(f'Завершена обработка версии {ver}')
        git_push_after_time(conf)
        terminate_script(conf)

    git_push(conf)
    logger.info('Завершен перенос истории хранилища в git')


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
# из хранилища
def update_to_storage_version(conf: dict, version_for_load: int):
    oc_command = update_to_storage_version_command(conf, version_for_load)
    execute_command(conf, oc_command)


def dump_configuration_to_git_command(conf: dict) -> OCcommand:
    global first_dump

    onec = conf['onec']
    git_options = conf['git']
    command_line = get_onec_command_line(conf, 'DESIGNER')

    dump_param_str = '/DumpConfigToFiles "{}"'.format(git_options['configuration_src_path'])

    oc_command = OCcommand()
    if first_dump:
        oc_command.command_line = command_line + ' ' + dump_param_str
    else:
        oc_command.command_line = command_line + ' ' + dump_param_str + ' -update'
    oc_command.desc = 'Выгрузка в git'
    oc_command.time_out = onec['dump_timeout']
    oc_command.successful_msg = ''

    return oc_command


def git_author_for_version(conf: dict, author: str) -> str:
    git_options = conf['git']
    storage = conf['storage']

    authors = storage['authors']
    default_mail = git_options['default_user_email']
    for val in authors:
        if author == val['user']:
            return '{author} <{mail}>'.format(author=author, mail=val['email'])

    return '{author} <{mail}>'.format(author=author, mail=default_mail)


def get_commit_label(conf: dict, version_for_dump: int, version_data: dict) -> str:
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
    label = f'{commit_msg_prefix} ver:{version_for_dump}; {ver_label}; \n {comment}\n\n' \
            f'{added_obj} {changed_obj}\n'
    logger.info('Сообщение для git commit; %s', label)

    return label


# выгружает основную конфигурацию в git и выполняет commit
# от имени пользователя поместившего версию в хранилище
def dump_configuration_to_git(conf: dict, version_for_dump: int, version_data: dict):
    oc_command = dump_configuration_to_git_command(conf)
    execute_command(conf, oc_command)

    logger.info('Начало git commit')
    git_commit_storage_version(conf, version_for_dump, version_data)
    logger.info('git commit - завершен')


def git_commit_storage_version(conf: dict, version_for_dump: int, version_data: dict):
    logger.info('Начало git add')
    git_options = conf['git']
    repo = git.Repo(git_options['path'], search_parent_directories=False)
    repo.index.add('*')
    logger.info('Завершен git add; %s', version_for_dump)

    ver_author = version_data['Author']
    git_author = git_author_for_version(conf, ver_author)
    label = get_commit_label(conf, version_for_dump, version_data)
    commit_stamp = datetime.strptime(version_data['CommitDate'] + ' ' + version_data['CommitTime'], "%d.%m.%Y %H:%M:%S")

    repo.git.commit('-m', label, author=git_author, date=commit_stamp)
    logger.info('Завершен git commit; %s', version_for_dump)


# сохраняет номер последней обработанной версии
# для того чтобы продолжить следующую загрузку
# со следующей
def save_last_version(conf: dict, last_version: int):
    storage_data_path = get_storage_data_path(conf)
    with open(storage_data_path, mode='w') as storage_data_file:
        json.dump({'last_version': last_version}, storage_data_file)

    logger.info('Сохранен номер обработанной версии; %s', storage_data_path)


def convert_storage_to_git(conf):
    try:
        last_version = get_last_storage_version(conf)

        logger.info('Запуск скрипта')
        restore_bd_configuration(conf)
        create_storage_report(conf, last_version)
        create_storage_history(conf)
        scan_history(conf)

        logger.debug('Завершение скрипта')
    except Exception as e:
        logger.exception('Script error')
        raise e


if __name__ == '__main__':
    conf = init_configuration()
    start_logger(conf)
    convert_storage_to_git(conf)

    # доп.строка для остановки при отладке
    pass
