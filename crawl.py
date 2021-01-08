import argparse
import logging
import os
import sys
import time
from datetime import datetime

from API import PlayStore, FDroid
from API.Exceptions import Maximum, Wait, Retry, RequestError

LOGGER = logging.getLogger('Crawler')
LOGGER.setLevel(logging.INFO)

ALL = True


def _download(api, apps, downloaded, retry):
    """Given an API and previously discovered apps from the same API, downloads them.

    If the downloads fails, adds the app to a retry list that can be queried again if desired.

    Parameters
    ----------
    api : F-Droid.API or PlayStore.API
        The api to use.
        It will do most of the work for you.
    apps : list

    downloaded : set
        Contains all previously downloaded apps.
        It is necessary to check against as some applications may be contained in several subcategories
    retry : bool
        Specifies whether to add elements to the retry list or not.

    Returns
    -------
    list:
        Apps that were appended to the retry list and should be retried.
    list:
        Apps that responded busy, probably implying rate limiting.
        Those should be retried even if no retries per se are desired
    list:
        Apps that were successfully downloaded to filter against in the next iteration.
    """
    already_waited = False
    wait = 30
    retry_list = []
    busy_list = []
    local_success = set()
    for app in apps:
        name = app.package_name()
        if name in downloaded or name in local_success:
            continue
        try:
            api.download(app)
            local_success.add(app.package_name())
            already_waited = False
            wait = 30
        except Wait:
            busy_list.append(app)
            if already_waited:
                wait = wait * 2
            LOGGER.warning(f'Server was busy, waiting {wait} seconds')
            already_waited = True
            time.sleep(wait)
        except Retry:
            already_waited = False
            wait = 30
            if retry:
                retry_list.append(app)
                LOGGER.warning(f'Server said to retry for app {app.package_name()}')
            else:
                LOGGER.exception(f'App {name} failed a second time, skipping')
        except RequestError as e:
            LOGGER.error(f'{app.package_name()}\n\t{e}')
            retry_list.append(app)
    return retry_list, busy_list, local_success


def crawl(api):
    """Crawls the given API for Android applications.

    As the individual stores differ greatly, most of the implementation is located in the API itself.
    The crawler only handles retries and rate limiting, otherwise, it is just a wrapper for a few chained API calls.

    Parameters
    ----------
    api : F-Droid.API or PlayStore.API
        The api to use.
        It will do most of the work for you.

    """
    LOGGER.info(f'Crawling apps for {api.store}')
    app_lists = _discover_apps(api)
    retry = []
    busy = []
    downloaded = set()
    for app_list in app_lists:
        LOGGER.info(f'Started processing "{app_list.name()}"')
        local_retry, local_busy, local_success = _download(api, app_list, downloaded, retry=True)
        retry.extend(local_retry)
        busy.extend(local_busy)
        downloaded.update(local_success)
        LOGGER.info(f'Finished processing "{app_list.name()}"\n'
                    f'\tOut of {len(app_list)} apps, {len(local_success)} were downloaded.\n'
                    f'\t{len(local_retry)} will have to be retried and for {len(local_busy)}, the server was busy')
    LOGGER.info(f'Started processing previously failed apps')
    local_retry, local_busy, local_success = _download(api, busy, downloaded, retry=True)
    LOGGER.info(f'Finished processing the list of apps that previously responded with "busy"\n'
                f'\tOut of {len(busy)} apps, {len(local_success)} were downloaded.\n'
                f'\t{len(local_retry)} will have to be retried and for {len(local_busy)}, the server was busy')
    downloaded.update(local_success)
    if local_busy:
        LOGGER.info(f'There were still {len(local_busy)} times the server was busy.\n'
                    f'\tWaiting an hour before retrying those apps')
        total_wait = 0
        for i in range(60):
            time.sleep(60)
            total_wait += 1
            LOGGER.info(f'Waited {total_wait} of 60 minutes')
        LOGGER.info(f'Started processing the apps that previously failed twice')
        local_retry, local_busy_new, local_success = _download(api, local_busy, downloaded, retry=True)
        LOGGER.info(f'Finished processing the list of apps that previously responded with "busy" twice\n'
                    f'\tOut of {len(local_busy)} apps, {len(local_success)} were downloaded.\n'
                    f'\t{len(local_retry)} will have to be retried and for'
                    f' {len(local_busy_new)}, the server was busy again, skipping those apps now')
    retry.extend(local_retry)
    _download(api, retry, downloaded, retry=False)


def _discover_apps(api):
    """Discovers applications for the specific API.

    Parameters
    ----------
    api : F-Droid.API or PlayStore.API
        The api to use.
        It will do most of the work for you.

    Returns
    -------
    list:
        A list of AppLists.
    """
    categories = api.categories()
    subcategories = []
    for category in categories:
        subcategories.extend(api.subcategories(category))
    app_lists = []
    app_count = 0
    LOGGER.info(f'Found {len(subcategories)} subcategories for {len(categories)} categories')
    for subcategory in subcategories:
        app_list = api.discover_apps(subcategory)
        if not app_list:
            continue
        while ALL:
            try:
                app_list.more()
            except Maximum:
                LOGGER.info(f'Subcategory "{app_list.name()}" yielded {len(app_list)} apps')
                break
        app_lists.append(app_list)
        app_count += len(app_list)
    app_set = set()
    for app_list in app_lists:
        for app in app_list:
            app_set.add(app.package_name())
    LOGGER.info(f'{"#" * 60}\n'
                f'\tFinished discovering Apps!\n'
                f'\tGot {app_count} apps in {len(app_lists)} subcategories of {len(categories)} categories\n'
                f'\tOut of those {app_count} apps, {len(app_set)} apps had a unique package name\n'
                f'\t{"#" * 60}')
    return app_lists


def parse_args():
    """Parses arguments from stdin.

    Returns
    -------
    Namespace :
        The arguments parsed from stdin.
    """
    parser = argparse.ArgumentParser(description='Crawl an Android app store for apk files.')
    parser.add_argument('--store', dest='api', choices=['GooglePlay', 'F-Droid'], required=True,
                        help='Specifies the store to crawl. At the moment only Google Play is supported')
    parser.add_argument('--meta', dest='meta', required=False, action='store_const', default=False, const=True,
                        help='If set, no apps will be downloaded, but the meta_data will be saved')
    parser.add_argument('--basedir', dest='base_dir', type=str, default=os.getenv('HOME'),
                        required=False, help='Specifies the base path for both logs and apk_downloads')
    parser.add_argument('--credentials', dest='credentials', type=str, required=False, default=None,
                        help='Specifies the path to a credential file in .toml format.')
    return parser.parse_args()


def crawl_meta_data(api):
    """Crawls the api for metadata, but does not downloads any APK.

    Parameters
    ----------
    api : F-Droid.API or PlayStore.API
        The api to use.
        It will do most of the work for you.

    """
    app_lists = _discover_apps(api)
    for app_list in app_lists:
        sub_category = app_list.subcategory.proto
        for app in app_list:
            app.proto.category.CopyFrom(sub_category)
            LOGGER.info(app)
            app.write_to_file()
        LOGGER.info(sub_category)


if __name__ == '__main__':
    """Crawls an Android store for metadata and/or the apps themselves, depending on the arguments."""
    args = parse_args()
    preliminary_base_dir = args.base_dir
    if preliminary_base_dir[0] == '/':
        base_dir = preliminary_base_dir
    elif preliminary_base_dir[0] == '~':
        base_dir = preliminary_base_dir.replace('~', os.getenv('HOME'))
    else:
        base_dir = os.path.join(os.getcwd(), preliminary_base_dir)
    log_path = os.path.join(base_dir, 'logs')
    os.makedirs(log_path, exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(log_path, f'{datetime.now()}.log'), 'w+')
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.NOTSET)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt='{asctime} - {name} - {levelname}\n\t{message}', style='{')
    file_handler.setFormatter(formatter)
    stdout_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(stdout_handler)
    if args.api == 'GooglePlay' and args.credentials is None:
        LOGGER.error('Crawling the Google Play Store requires a credential file.')
        exit(1)
    api = {
        'GooglePlay': PlayStore.API,
        'F-Droid': FDroid.API
    }
    if args.meta:
        crawl_meta_data(api=api[args.api](args.credentials, base_dir=base_dir,logger=LOGGER))
    else:
        print(base_dir)
        crawl(api=api[args.api](args.credentials, base_dir=base_dir,
            logger=LOGGER))
