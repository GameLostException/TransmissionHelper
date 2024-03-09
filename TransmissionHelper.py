import argparse
import json
import logging
import os
import shutil
import sys
import textwrap
from pathlib import Path

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


class ListMode(Enum):
    ID = 'id'
    SIZE = 'size'
    SEED_RATIO = 'seed_ratio'
    CREATED = 'created'
    NAME = 'name'
    PROGRESS = 'progress'

    @staticmethod
    def from_str(label):
        match label:
            case 'id':
                return ListMode.ID
            case 'size':
                return ListMode.SIZE
            case 'seed_ratio':
                return ListMode.SEED_RATIO
            case 'created':
                return ListMode.CREATED
            case 'name':
                return ListMode.NAME
            case 'progress':
                return ListMode.PROGRESS
            case _:
                raise NotImplementedError


class TransmissionHelper:
    # Default minimum ratio needed to consider removing the torrent and its files
    MIN_SEED_RATIO = 3.0
    # Default minimum disk space to keep free. Nothing should be deleted if there is enough free space
    # 100*1024*1024*1024 is 100 GiB
    MIN_FREE_SPACE = 100 * 1024 * 1024 * 1024
    # Mount point to monitor space for, defaulted to the volume containing this script,
    # ideally the Transmission's download directory so it can also be used for
    # TODO possibly read the transmission config directly (/etc/transmission-daemon/settings.json)?
    TRANSMISSION_DOWNLOAD_DIR = __file__
    # Default logging location
    LOG_FILE_PATH = '.'
    # TODO add config_file as default static

    # Args parser config with detailed help
    parser = argparse.ArgumentParser(prog='TransmissionHelper',
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
    group.add_argument('-t', '--list_torrent',
                       choices=['id', 'size', 'seed_ratio', 'created', 'name'],
                       type=str,
                       nargs='?',
                       const='id',
                       help=textwrap.dedent('''\
                            blah'''))
    group.add_argument('-d', '--storage_delta',
                       type=str,
                       nargs='*',
                       help=textwrap.dedent('''\
                                Lists gaps between the Transmission torrent list and its downloads directories.
                                This is based on the configured directories but can be overridden by passing several
                                paths to this option.
                                Removal of the non-matching items can be actioned via the --execute option.'''))
    group.required = True
    parser.add_argument('-x', '--execute',
                        action='store_true',
                        help=textwrap.dedent('''\
                            By default no impacting action is taken toward Transmission, this flag
                            is needed to overrides this behaviour'''))
    parser.add_argument('-f', '--config_file',
                        help=textwrap.dedent('''\
                                Full path of the config file to use, defaults to ./config.json'''))
    parser.add_argument('-r', '--min_ratio',
                        type=float,
                        help=textwrap.dedent('''\
                                    Minimum seeding ratio used as a filter for the listing and cleaning options'''))
    parser.add_argument('-s', '--min_free_space',
                        type=int,
                        help=textwrap.dedent('''\
                                        Minimum desired free space in Bytes, 
                                        applies to the listing and cleaning options'''))
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help=textwrap.dedent('''\
                                Enable debug-level logging, both on stdout and logging file.'''))

    def __init__(self):
        # Default logging
        self.logger = logging.getLogger(__name__)
        std_handler = logging.StreamHandler(stream=sys.stdout)
        std_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
        self.logger.addHandler(std_handler)
        self.logger.setLevel(logging.INFO)
        self.log_file_path = self.LOG_FILE_PATH

        # Torrent lists
        self.torrent_list = []
        self.torrent_list_min_ratio = []
        self.torrent_list_min_ratio_size = 0
        self.torrent_list_min_free_space = []
        self.torrent_list_min_free_space_size = 0

        # Transmission
        self.client = None
        self.transmission_download_dir = self.TRANSMISSION_DOWNLOAD_DIR
        self.transmission_incomplete_dir = None

        # Configuration
        self.config_file = 'config.json'
        self.config = None

    def configure(self):
        # Load configuration file
        with open(self.config_file, 'r') as conf:
            try:
                self.config = json.load(conf)
            except Exception as e:
                self.logger.error('Could not parse config file \'%s\', parser returned \'%s\'', self.config_file, e)
                exit(2)

        # Setup the file logger
        logfile_conf_success = False
        if os.access(self.config['logging']['file_path'], os.W_OK | os.X_OK):
            self.log_file_path = self.config['logging']['file_path']
            logfile_conf_success = True
        file_handler = logging.FileHandler(self.log_file_path + '/' + self.config['logging']['file_name'])
        file_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
        self.logger.addHandler(file_handler)
        if not logfile_conf_success:
            self.logger.warning('Log file path \'%s\' is not writable, falling back to current script location \'%s\'',
                                self.config['logging']['file_path'], os.path.dirname(__file__))

        # Set up the download and incomplete directory
        ## Download
        dl_conf = self.config['transmission']['download_dir']
        dl_dir_conf_success = False
        if os.access(dl_conf, os.F_OK | os.R_OK | os.X_OK):
            self.transmission_download_dir = dl_conf
            dl_dir_conf_success = True
        if not dl_dir_conf_success:
            self.logger.warning('Transmission download directory path \'%s\' is not readable, '
                                'falling back to current script location \'%s\'',
                                dl_conf,
                                os.path.dirname(self.transmission_download_dir))
        ## Incomplete
        inc_conf = self.config['transmission']['incomplete_dir']
        if not inc_conf:
            self.logger.info(
                'Transmission incomplete directory not setup, make sure your Transmission configuration doesn\'t '
                'have one.')
        elif not os.access(inc_conf, os.F_OK):
            self.logger.warning('Transmission incomplete directory \'%s\' does not exist, ignoring.', inc_conf)
        elif os.access(inc_conf, os.R_OK | os.X_OK):
            self.transmission_incomplete_dir = inc_conf

    def __connect(self):
        try:
            self.client = Client(host=self.config['transmission']['host'],
                                 port=self.config['transmission']['port'],
                                 username=self.config['transmission']['username'],
                                 password=self.config['transmission']['password'])
        except:
            self.logger.error('Could not connect to Transmission server with host=%s, port=%s, login=%s, pwd=%s',
                              self.config['transmission']['host'],
                              self.config['transmission']['port'],
                              self.config['transmission']['username'],
                              self.config['transmission']['password'])
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
        return shutil.disk_usage(self.TRANSMISSION_DOWNLOAD_DIR)[2] > self.MIN_FREE_SPACE

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
        if self.__is_enough_free_space():
            self.logger.info("There is more than %s of free space already (%s), no need to clean-up torrents yet.",
                             self.__human_readable_size(self.MIN_FREE_SPACE),
                             self.__get_human_disk_free_space())
            exit(0)
        # Get the torrents data now
        self.__connect()
        self.__get_torrents_data()
        # Check we have material to clean
        if self.torrent_list_min_ratio_size == 0.0:
            self.logger.info('There is no eligible torrent to clean-up, consider changing the sorting and '
                             'filters criteria of the Transmission torrent list.')
            exit(1)
        # Clean according to the passed CleanMode
        match clean_mode:
            case CleanMode.MIN_FREE_SPACE:
                # Compute the space we need to free to obtain MIN_FREE_SPACE
                space_to_free = self.MIN_FREE_SPACE - self.__get_disk_free_space()
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
        return shutil.disk_usage(self.TRANSMISSION_DOWNLOAD_DIR)[2]

    def __get_human_disk_free_space(self):
        return self.__human_readable_size(shutil.disk_usage(self.TRANSMISSION_DOWNLOAD_DIR)[2])

    # TODO WIP, need a proper display of the table and setting the sorting options right
    def list_torrents(self):
        self.__connect()
        self.__get_torrents_data()
        matrix_data = self.__get_torrent_list_as_matrix(self.torrent_list)
        # TODO link the sorting types to the ad-hoc cols
        # TODO make sorting by size work (do we add a hidden byte col?)
        matrix_data[0].sort(key=lambda item: item[0])
        print(matrix_data[1])
        for row in matrix_data[0]:
            print('| {:4d} | {:80.80s} | {:%Y-%m-%d %H:%M:%S} | {:10.10s} | {:.0f} | {:.1f} | {:s} |'.format(*row))
        print('%d torrents, %s on disk.' % (len(matrix_data[0]), self.__human_readable_size(matrix_data[2])))

    def storage_delta(self, execute):
        self.__connect()
        self.__get_torrents_data()
        if not os.path.isdir(self.transmission_download_dir):
            self.logger.error('\'%s\' is not a directory, aborting.', self.transmission_download_dir)
            exit(3)
        if not os.access(self.transmission_download_dir, os.R_OK | os.X_OK):
            self.logger.error('Download directory \'%s\' is not readable, aborting.', self.transmission_download_dir)
            exit(3)
        # Download dir
        dl_extra_list = []
        dl_dir_list = os.listdir(self.transmission_download_dir)
        item_found = False
        for item in dl_dir_list:
            for torrent in self.torrent_list:
                if torrent.name == item:
                    item_found = True
                    break
            if not item_found:
                dl_extra_list.append(item)
            item_found = False

        dl_extra_list.sort()
        for e in dl_extra_list:
            print(e)
        self.logger.info('Found %s extra items in the download dir \'%s\' (%s torrents, %s items in dl dir)',
                         len(dl_extra_list), self.transmission_download_dir, len(self.torrent_list), len(dl_dir_list))
        if execute:
            self.logger.debug('Deleting files:')
            for f in dl_extra_list:
                file_path_to_delete = self.transmission_download_dir + '/' + f
                self.logger.debug('Deleting %s' + file_path_to_delete)
                try:
                    Path(file_path_to_delete).unlink()
                    print("File deleted successfully.")
                except FileNotFoundError:
                    print("File not found.")
                except PermissionError:
                    print("Permission denied. Unable to delete the file.")
                except Exception as e:
                    print("An error occurred:", e)

            # Incomplete dir

    @staticmethod
    def __get_torrent_list_as_matrix(torrent_list):
        torrent_matrix = []
        sum_size = 0
        for torrent in torrent_list:
            torrent_matrix.append([torrent.id,
                                   torrent.name,
                                   torrent.added_date,
                                   # str(torrent.total_size),
                                   TransmissionHelper.__human_readable_size(torrent.total_size),
                                   torrent.progress,
                                   torrent.ratio,
                                   torrent.status])
            sum_size += torrent.total_size
        return torrent_matrix, ['ID', 'FileName', 'Added Date', 'Size', 'D/L', 'Ratio', 'Status'], sum_size


def main():
    transmission_helper = TransmissionHelper()
    args = transmission_helper.parser.parse_args()

    # Logging setup
    if vars(args).get('verbose'):
        transmission_helper.logger.setLevel(logging.DEBUG)
    if vars(args).get('config_file'):
        transmission_helper.config_file = args.config_file
    # TODO
    # Min ratio setup
    if vars(args).get('min-ratio'):
        transmission_helper.MIN_SEED_RATIO = args.min_ratio
    # TODO
    # Min free space setup
    if vars(args).get('min-free-space'):
        transmission_helper.MIN_FREE_SPACE = args.min_free_space

    transmission_helper.configure()

    # Actions
    if vars(args).get('list_torrent'):
        transmission_helper.list_torrents()
    elif vars(args).get('clean_mode'):
        transmission_helper.cleanup(CleanMode.from_str(args.clean_mode), args.execute)
    # elif vars(args).get('storage_delta'):
    elif args.storage_delta is not None:
        transmission_helper.storage_delta(args.execute)
    else:
        print(transmission_helper.parser.format_help())
        exit(0)


if __name__ == "__main__":
    main()
