import argparse
import json
import logging
import shutil
import sys
import textwrap

from transmission_rpc import Client
from enum import Enum


class CleanMode(Enum):
    # Clean all the torrents that have seeded at least for the MIN_SEED_RATIO value
    MIN_SEED_RATIO = 'seed-ratio'
    # Clean the torrents that have seeded at least for the MIN_SEED_RATIO value up to what is needed to free
    # space up to the MIN_FREE_SPACE value
    MIN_FREE_SPACE = 'free-space'

    @staticmethod
    def from_str(label):
        match label:
            case 'seed_ratio':
                return CleanMode.MIN_SEED_RATIO
            case 'free_space':
                return CleanMode.MIN_FREE_SPACE
            case _:
                raise NotImplementedError


class TransmissionHelper:
    # The minimum ratio needed to consider removing the torrent and its files
    MIN_SEED_RATIO = 3.0
    # The minimum disk space to keep free. Nothing should be deleted if there is enough free space
    # 100*1024*1024*1024 is 100 GiB
    MIN_FREE_SPACE = 100 * 1024 * 1024 * 1024
    # Mount point to monitor space for
    MOUNT_PT = __file__
    # MOUNT_PT = "/srv/dev-disk-by-uuid-e49a2a19-0dc1-4774-b3f6-27ba1770ded1"
    # Logging location
    LOG_FILE_PATH = '/tmp/transmissionHelper.log'
    # LOG_FILE_PATH = '/var/log/transmissionHelper.log'

    parser = argparse.ArgumentParser(
        prog='TransmissionHelper',
        description='Suite of CLI utilities for Transmission',
        epilog='gamelostexcpetion@gmail.com',
        formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-c', '--clean_mode',
                       choices=['seed_ratio', 'free_space'],
                       type=str,
                       nargs='?',
                       const='free_space',
                       help=textwrap.dedent('''\
                            Cleans the torrents using either of 2 modes:
                                * seedRatio:    removes all the torrents that have already seeded above
                                                the hard-configured value.
                                * freeSpace:    removes all the torrents needed to free space up to the hard-configured
                                                value provided they have a minimum seeding ratio.'''))
    group.add_argument('-l', '--list_sort',
                       choices=['id', 'size', 'seed_ratio', 'created', 'name'],
                       type=str,
                       nargs='?',
                       const='id',
                       help=textwrap.dedent('''\
                            blah'''))
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help=textwrap.dedent('''\
                                Enable debug-level logging, both on stdout and logging file.'''))
    parser.add_argument('-x', '--execute', action='store_true',
                        help=textwrap.dedent('''\
                            By default no impacting action is taken toward Transmission, this flag
                            is needed to overrides this behaviour'''))

    def __init__(self):
        # Logging
        self.logger = logging.getLogger(__name__)
        file_handler = logging.FileHandler(self.LOG_FILE_PATH)
        file_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
        std_handler = logging.StreamHandler(stream=sys.stdout)
        std_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
        self.logger.addHandler(file_handler)
        self.logger.addHandler(std_handler)
        self.logger.setLevel(logging.INFO)
        # Torrent lists
        self.torrent_list = []
        self.torrent_list_min_ratio = []
        self.torrent_list_min_ratio_size = 0
        self.torrent_list_min_free_space = []
        self.torrent_list_min_free_space_size = 0
        # Transmission client's credentials
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)
        try:
            self.client = Client(host=config.get('host'),
                                 port=config.get('port'),
                                 username=config.get('credentials').get('username'),
                                 password=config.get('credentials').get('password'))
        except:
            self.logger.error('Could not connect to Transmission server')
            exit(-1)

    # Helper function to display bytes sizes in a human friendly way
    @staticmethod
    def __human_readable_size(size, decimal_places=2):
        for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
            if size < 1024.0 or unit == 'PiB':
                break
            size /= 1024.0
        return f"{size:.{decimal_places}f} {unit}"

    def __is_enough_free_space(self):
        return shutil.disk_usage(self.MOUNT_PT)[2] > self.MIN_FREE_SPACE

    def __get_torrents_data(self):
        # Connect client
        # Get the Transmission torrent list
        self.torrent_list = self.client.get_torrents()
        # Order by highest seeding ratio first
        self.torrent_list.sort(reverse=True, key=lambda torrent: torrent.upload_ratio)
        # Get a sublist of only the torrents having a minimum seeding ratio
        self.torrent_list_min_ratio = list(filter(lambda torrent: torrent.upload_ratio >= self.MIN_SEED_RATIO,
                                                  self.torrent_list))
        # Compute the total size of the top_uploaded_list
        for t in self.torrent_list_min_ratio:
            self.torrent_list_min_ratio_size += t.total_size

    def cleanup(self, clean_mode, execute):
        if not execute:
            self.logger.info("Running in preview mode, no change request is actually sent to Transmission.")
        # Check whether we have enough free space
        if self.__is_enough_free_space():
            self.logger.info("There is more than %s of free space already, no need to clean-up torrents.",
                             self.__human_readable_size(self.MIN_FREE_SPACE))
            exit(0)
        # Get the torrents data now
        self.__get_torrents_data()
        # Check we have material to clean
        if self.torrent_list_min_ratio_size == 0.0:
            self.logger.info(
                "There is no eligible torrent to clean-up, consider changing the sorting and filters criteria "
                "of the Transmission torrent list.")
            exit(1)
        # Clean according to the passed CleanMode
        match clean_mode:
            case CleanMode.MIN_FREE_SPACE:
                # Compute the space we need to free to obtain MIN_FREE_SPACE
                space_to_free = self.MIN_FREE_SPACE - shutil.disk_usage(self.MOUNT_PT)[2]
                total_torrents_to_clean = 0
                for t in self.torrent_list_min_ratio:
                    if total_torrents_to_clean < space_to_free:
                        total_torrents_to_clean += t.total_size
                        self.torrent_list_min_free_space.append(t)
                    else:
                        break
                # Get the total size of torrent gathered
                for t in self.torrent_list_min_free_space:
                    self.torrent_list_min_free_space_size += t.total_size
                self.logger.info("Cleaning by freeSpace: "
                                 "%s are already free, need at least %s more to reach the %s mark. "
                                 "Will actually free %s more to reach a total of %s free space.",
                                 self.__get_human_disk_free_space(),
                                 self.__human_readable_size(space_to_free),
                                 self.__human_readable_size(self.MIN_FREE_SPACE),
                                 self.__human_readable_size(self.torrent_list_min_free_space_size),
                                 self.__human_readable_size(self.__get_disk_free_space()
                                                            + self.torrent_list_min_free_space_size))
                min_free_space_torrents_id_list = [tx.id for tx in list(self.torrent_list_min_free_space)]
                self.logger.debug("Target is %d torrents with IDs %s",
                                  len(self.torrent_list_min_free_space),
                                  min_free_space_torrents_id_list)
                if execute:
                    self.logger.info('Removing %d torrents for a total of %s', len(min_free_space_torrents_id_list),
                                     self.__human_readable_size(self.torrent_list_min_free_space_size))
                    self.__remove_torrents(min_free_space_torrents_id_list)
            case CleanMode.MIN_SEED_RATIO:
                self.logger.info("Cleaning by seedRatio: "
                                 "%s are already free, "
                                 "removing all torrents above %.1f seeding ratio will free an additional %s space "
                                 "for a total of %s free space.",
                                 self.__get_human_disk_free_space(),
                                 self.MIN_SEED_RATIO,
                                 self.__human_readable_size(self.torrent_list_min_ratio_size),
                                 self.__human_readable_size(self.__get_disk_free_space()
                                                            + self.torrent_list_min_ratio_size))
                min_ratio_torrents_id_list = [tx.id for tx in list(self.torrent_list_min_ratio)]
                self.logger.debug("Target is %d torrents for a total of %s",
                                  len(self.torrent_list_min_ratio),
                                  self.__human_readable_size(self.torrent_list_min_ratio_size))
                if execute:
                    self.logger.info('Removing torrents with IDs ', min_ratio_torrents_id_list)
                    self.__remove_torrents(min_ratio_torrents_id_list)
        # Check for the cleaning results
        if self.__get_disk_free_space() >= self.MIN_FREE_SPACE:
            self.logger.info("There is now %s of free space, no more cleaning action needed for now.",
                             self.__get_human_disk_free_space())
        else:
            self.logger.info("There is now %s of free space which is still below the minimum that has been setup, %s. "
                             "Consider running this script with either of a lower minimum seeding ratio (now %.1f), "
                             "a lesser minimum free disk space value, and make sure it executes (-x option).",
                             self.__get_human_disk_free_space(),
                             self.__human_readable_size(self.MIN_FREE_SPACE),
                             self.MIN_SEED_RATIO)

    def __remove_torrents(self, torrent_list):
        self.client.remove_torrent(ids=torrent_list, delete_data=True)

    def __get_disk_free_space(self):
        return shutil.disk_usage(self.MOUNT_PT)[2]

    def __get_human_disk_free_space(self):
        return self.__human_readable_size(shutil.disk_usage(self.MOUNT_PT)[2])

    # TODO WIP, need a proper display of the table and setting the sorting options right
    def list_torrents(self):
        self.__get_torrents_data()
        matrix = self.__get_torrent_list_as_matrix(self.torrent_list)
        for row in matrix:
            self.logger.info('| {:6d} | {:50.50s} | {:%Y-%m-%d %H:%M:%S} | {:.2f} | {:.2f} | {:6s} |'.format(*row))

    @staticmethod
    def __get_torrent_list_as_matrix(torrent_list):
        torrent_matrix = []
        for torrent in torrent_list:
            torrent_matrix.append([torrent.id, torrent.name, torrent.added_date, torrent.progress, torrent.ratio,
                                   torrent.status])
        return torrent_matrix


def main():
    h = TransmissionHelper()
    args = h.parser.parse_args()

    # Logging setup
    if vars(args).get('verbose'):
        h.logger.setLevel(logging.DEBUG)
    # Min ratio setup
    if vars(args).get('min-ratio'):
        h.MIN_SEED_RATIO = args.min_ratio
    # Min free space setup
    if vars(args).get('min-free-space'):
        h.MIN_FREE_SPACE = args.min_free_space

    # Actions
    if vars(args).get('list_sort'):
        h.list_torrents()
    elif vars(args).get('clean_mode'):
        h.cleanup(CleanMode.from_str(args.clean_mode), args.execute)
    else:
        print(h.parser.format_help())
        exit(1)


if __name__ == "__main__":
    main()
