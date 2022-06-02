import argparse
import os
import json
import subprocess
import git

from datetime import datetime



"""import sqlite3"""


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


def get_storage_data_path() -> str:
    return os.path.join(os.getcwd(), "storage_data.json")


# получает номер последней версии, которую удалось
# прочитать из хранилища.
# продолжать чтение надо с версии последняя+1
def get_last_storage_version() -> int:
    storage_data_path = get_storage_data_path()
    if os.path.exists(storage_data_path):
        with open(storage_data_path, mode='r') as storage_data_file:
            storage_data = json.load(storage_data_file)
            last_version = storage_data['last_version']
    else:
        last_version = 0

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
        user = '/N{}'.format(info_base['user'])
        password = '/P{}'.format(info_base['password'])
    # /DumpResult {dump_path}
    onec_command_line = '{start_path} {start_type} {wa_flag} /DisableStartupDialogs {user_name} ' \
                        '{passwd} /L ru /VL ru /IBConnectionString {connection_string} ' \
                        '/Out {log_path} -NoTruncate ' \
                        ' '.format(start_path=onec['start_path'],
                                   start_type=start_type,
                                   wa_flag=wa,
                                   user_name=user,
                                   passwd=password,
                                   connection_string=info_base['connection_string'],
                                   # dump_path=onec['result_dump_path'],
                                   log_path=onec['log_file_path'])
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
def create_storage_report_command(conf: dict, last_version: int) -> str:
    command_line = get_onec_command_line(conf, 'DESIGNER')

    storage = conf['storage']
    if storage['password'] == "":
        passwd_flag = ""
    else:
        passwd_flag = storage['password']

    start_version = last_version + 1

    report_param_str = '/ConfigurationRepositoryF {storage_path} ' \
                       '/ConfigurationRepositoryN {storage_user} {storage_passwd_flag} ' \
                       '/ConfigurationRepositoryReport {report_path} -NBegin {ver_num} ' \
                       ' '.format(storage_path=storage['path'],
                                  storage_user=storage['user'],
                                  storage_passwd_flag=passwd_flag,
                                  report_path=storage['report_path'],
                                  ver_num=start_version)

    create_report_command = command_line + ' ' + report_param_str
    return create_report_command


def create_storage_report(conf: dict, last_version: int):
    onec = conf['onec']
    command_line = create_storage_report_command(conf, last_version)
    print("Начато формирование отчета по хранилищу:\n" + command_line)
    subprocess.run(command_line, shell=False, timeout=onec['timeout'])
    print("Завершено формирование отчета по хранилищу:\n" + command_line)

# example:
# "c:\Program Files\1cv8\8.3.18.1289\bin\1cv8.exe" ENTERPRISE /WA- /DisableStartupDialogs
# /NСервисРаботыСХранилищем /P…..  /L ru /VL ru
# /F "C:\Users\milut\Documents\StorageReceiver"
# /Execute "C:\1C\Работа с хранилищем\ОтчетПоХранилищуВjson.epf"
# /C "C:\1C\Работа с хранилищем\storage_report003.mxl;C:\1C\Работа с хранилищем\storage_history.json"
# /Out "C:\Users\milut\Documents\1C\log.txt" -NoTruncate
def create_storage_history_command(conf: dict) -> str:
    command_line = get_onec_command_line(conf, 'ENTERPRISE')
    onec = conf['onec']
    storage = conf['storage']

    convert_param_str = '/Execute {converter_path} ' \
                        '/C"{report_path};{history_path}" ' \
                        ' '.format(converter_path=onec['report_convert_processor_path'],
                                   report_path=storage['report_path'],
                                   history_path=storage['json_report_path'])

    convert_report_command = command_line + ' ' + convert_param_str
    return convert_report_command


def create_storage_history(conf: dict):
    onec = conf['onec']
    command_line = create_storage_history_command(conf)
    print("Начато формирование файла истории хранилища:\n", command_line)
    subprocess.run(command_line, shell=False, timeout=onec['timeout'])
    print("Завершено формирование файла истории хранилища:\n", command_line)


# преобразует json файл с историей хранилища
# в упорядоченный список структур, которые описывают версии
# хранилища. Далее по данному списку выполняется выгрузка
# истории хранилища в git
def read_storage_history(conf: dict) -> dict:
    history_path = conf['storage']['json_report_path']
    with open(history_path, 'r', encoding="utf_8_sig") as history_file:
        history_data = json.load(history_file)

    return history_data


# проходит по версиям хранилища от меньшей к большей
# и выгружает данные каждой версии из истории в git
def scan_history(conf: dict):
    history_data = read_storage_history(conf)
    versions = list()
    last_version: int = 0
    for key in history_data.keys():
        versions.append(int(key))

    versions.sort()
    for ver in versions:
        version_data = history_data[str(ver)]
        update_to_storage_version(conf, ver)
        dump_configuration_to_git(conf, ver, version_data)
        last_version = ver
        save_last_version(last_version)


def update_to_storage_version_command(conf: dict, version_for_load: int):
    command_line = get_onec_command_line(conf, 'DESIGNER')

    storage = conf['storage']
    if storage['password'] == "":
        passwd_flag = ""
    else:
        passwd_flag = storage['password']

    update_param_str = '/ConfigurationRepositoryF {storage_path} ' \
                       '/ConfigurationRepositoryN {storage_user} {storage_passwd_flag} ' \
                       '/ConfigurationRepositoryUpdateCfg -force -v {ver_num} ' \
                       ' '.format(storage_path=storage['path'],
                                  storage_user=storage['user'],
                                  storage_passwd_flag=passwd_flag,
                                  ver_num=version_for_load)

    update_command = command_line + ' ' + update_param_str
    return update_command


# обновляет основную конфигурацию до указанной версии
# из хранилища
def update_to_storage_version(conf: dict, version_for_load: int):
    onec = conf['onec']
    command_line = update_to_storage_version_command(conf, version_for_load)
    subprocess.run(command_line, shell=False, timeout=onec['update_timeout'])


def dump_configuration_to_git_command(conf: dict) -> str:
    command_line = get_onec_command_line(conf, 'DESIGNER')
    git_options = conf['git']

    dump_param_str = '/DumpConfigToFiles {} '.format(git_options['configuration_src_path'])

    dump_command = command_line + ' ' + dump_param_str
    return dump_command


def git_author_for_version(conf: dict, author: str) -> str:
    git_options = conf['git']
    storage = conf['storage']

    authors = storage['authors']
    default_mail = git_options['default_user_email']
    for val in authors:
        if author == val['user']:
            return '{author} <{mail}>'.format(author=author, mail=val['email'])

    return '{author} <{mail}>'.format(author=author, mail=default_mail)


# выгружает основную конфигурацию в git и выполняет commit
# от имени пользователя поместившего версию в хранилище
def dump_configuration_to_git(conf: dict, version_for_dump: int, version_data: dict):
    onec = conf['onec']

    command_line = dump_configuration_to_git_command(conf)
    subprocess.run(command_line, shell=False, timeout=onec['dump_timeout'])
    git_commit_storage_version(conf, version_for_dump, version_data)

def git_commit_storage_version(conf: dict, version_for_dump: int, version_data: dict):
    git_options = conf['git']
    repo = git.Repo(git_options['path'])
    repo.index.add('*')

    ver_author = version_data['Author']
    git_author = git_author_for_version(conf, ver_author)
    ver_label = version_data['Version']
    comment = version_data['CommitMessage']
    changed_obj = 'Изменено:\n'
    for val in version_data['ChangedObjects']:
        changed_obj = changed_obj + ' ' + val + '\n'

    added_obj = 'Добавлено:\n'
    for val in version_data['AddedObjects']:
        added_obj = added_obj + ' ' + val + '\n'

    commit_stamp = datetime.strptime(version_data['CommitDate'] + ' ' + version_data['CommitTime'], "%d.%m.%Y %H:%M:%S")
    label = f'storage ver:{version_for_dump}{ver_label}: {comment}\n\n' \
            f'{added_obj} {changed_obj}\n'

    repo.git.commit('-m', label, author=git_author, date=commit_stamp)


# сохраняет номер последней обработанной версии
# для того чтобы продолжить следующую загрузку
# со следующей
def save_last_version(last_version: int):
    storage_data_path = get_storage_data_path()
    with open(storage_data_path, mode='w') as storage_data_file:
        json.dump({'last_version': last_version}, storage_data_file)


if __name__ == '__main__':
    conf = init_configuration()
    last_version = get_last_storage_version()

    # TODO: пока не ясно какие данные необходимо сохранять
    # по результатам работы скрипта
    # на данный момент обязательно к сохранению: номер последней прочитанной из хранилища версии

    # conn = sqlite3.connect(os.path.join(os.getcwd(), "storage_history.db"))
    # print(sqlite3.version)

    create_storage_report(conf, last_version)
    create_storage_history(conf)
    read_storage_history(conf)
    scan_history(conf)

    pass
