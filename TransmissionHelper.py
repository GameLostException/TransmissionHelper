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
    MIN_SEED_RATIO = 0.0  # TODO make it overrideable by the ad-hoc option
    # Default minimum disk space to keep free. Nothing should be deleted if there is enough free space
    # 100*1024*1024*1024 is 100 GiB
    MIN_FREE_SPACE = 100 * 1024 * 1024 * 1024
    # Mount point to monitor space of, defaulted to the volume containing this script,
    # ideally the Transmission's download directory so it can also be used for
    # TODO possibly read the transmission config directly (/etc/transmission-daemon/settings.json)?
    TRANSMISSION_COMPLETE_DIR = __file__
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
                       action='store_true',
                       help=textwrap.dedent('''\
                            Cleans the torrents using the following criterion:
                                1. freeSpace:   removes all the torrents needed to free space up to the configured
                                                value.
                                2. seedRatio:   restrict the removal of the torrents to those not having already seeded above
                                                the configured "min_ratio" value.'''))
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
                            is needed to overrides this behaviour.'''))
    parser.add_argument('-f', '--config_file',
                        help=textwrap.dedent('''\
                                Full path of the config file to use, defaults to ./config.json'''))
    parser.add_argument('-r', '--min_ratio',
                        type=float,
                        help=textwrap.dedent('''\
                                    Minimum seeding ratio used for the cleaning by seed_ratio option, defaults to 0.'''))
    parser.add_argument('-s', '--min_free_space',
                        type=int,
                        help=textwrap.dedent('''\
                                        Minimum desired free space in Bytes used for the cleaning by free_space option, 
                                        defaults to 107374182400 B (100 GiB).'''))
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help=textwrap.dedent('''\
                                Enable debug-level logging, both on stdout and logging file.'''))

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)

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
        self.torrent_list_space = 0

        # Transmission
        self.client = None
        self.transmission_complete_dir = self.TRANSMISSION_COMPLETE_DIR
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
            self.transmission_complete_dir = dl_conf
            dl_dir_conf_success = True
        if not dl_dir_conf_success:
            self.logger.warning('Transmission download directory path \'%s\' is not readable, '
                                'falling back to current script location \'%s\'',
                                dl_conf,
                                os.path.dirname(self.transmission_complete_dir))
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
        return self.__get_disk_free_space() > self.MIN_FREE_SPACE

    def __get_torrents(self):
        # Connect client
        self.__connect()
        # Get the Transmission torrent list
        self.torrent_list = self.client.get_torrents()
        # Order by highest seeding ratio first, regardless of criteria
        self.torrent_list.sort(reverse=True, key=lambda torrent: torrent.upload_ratio)
        # Compute the total size of the list
        for t in self.torrent_list:
            self.torrent_list_space += t.total_size

    def cleanup(self, execute):
        # We check if a clean is needed and exit otherwise
        if self.__get_disk_free_space() >= self.MIN_FREE_SPACE:
            self.logger.info("There is already more free space (%s) than the minimum required (%s), aborting.",
                             self.__human_readable_size(self.__get_disk_free_space()),
                             self.__human_readable_size(self.MIN_FREE_SPACE))
            exit(0)

        if not execute:
            self.logger.info("Running in preview mode, no deletion request will actually be sent to Transmission.")

        # Get the torrents data
        self.__get_torrents()

        space_to_free = self.MIN_FREE_SPACE - self.__get_disk_free_space()
        cleanable_space = 0
        torrents_to_clean = []
        self.logger.info("%s are already free so we need at least %s more to reach the %s mark.",
                         self.__get_human_disk_free_space(), self.__human_readable_size(space_to_free),
                         self.__human_readable_size(self.MIN_FREE_SPACE))
        for t in self.torrent_list:
            # As long as we need to free space...
            if cleanable_space < space_to_free:
                # and as long as the min_ratio allows...
                if t.ratio > self.MIN_SEED_RATIO:
                    # We collect the current torrent for further deletion
                    cleanable_space += t.total_size
                    torrents_to_clean.append(t)
            else:
                break

        # Case where we can't find enough torrent to clean
        if cleanable_space < space_to_free:
            self.logger.info(
                "There is not enough eligible torrents to clean (%d among %d) to reach the %s free space mark. "
                "Consider lowering the minimum seeding ratio (now %.1f).", len(torrents_to_clean),
                len(self.torrent_list),
                self.__human_readable_size(self.MIN_FREE_SPACE), self.MIN_SEED_RATIO)
            if len(torrents_to_clean) == 0:
                exit(0)

        # Regardless, we now clean what we can
        self.logger.info("%d torrents will be deleted to free %s more and reach a total of %s free space.",
                         len(torrents_to_clean),
                         self.__human_readable_size(cleanable_space),
                         self.__human_readable_size(space_to_free + self.__get_disk_free_space()))
        torrents_to_clean_id_list = [tx.id for tx in list(torrents_to_clean)]

        if execute:
            self.__remove_torrents(torrents_to_clean_id_list)

        # Check for the cleaning results
        if self.__get_disk_free_space() >= self.MIN_FREE_SPACE:
            self.logger.info("There is now %s of free space, no more cleaning action needed for now.",
                             self.__get_human_disk_free_space())
        else:
            self.logger.info("There is now %s of free space which is still below the minimum %s that has been setup. "
                             "Consider running this script with a lower minimum seeding ratio (now %.1f) and/or "
                             "a lesser minimum free disk space value (now ), and make sure it executes (-x option).",
                             self.__get_human_disk_free_space(),
                             self.__human_readable_size(self.MIN_FREE_SPACE),
                             self.MIN_SEED_RATIO)

    def __remove_torrents(self, torrent_list):
        self.client.remove_torrent(ids=torrent_list, delete_data=True)

    def __get_disk_free_space(self):
        return shutil.disk_usage(self.transmission_complete_dir)[2]

    def __get_human_disk_free_space(self):
        return self.__human_readable_size(self.__get_disk_free_space())

    # TODO WIP, need a proper display of the table and setting the sorting options right
    def list_torrents(self):
        self.__connect()
        self.__get_torrents()
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
        self.__get_torrents()
        if not os.path.isdir(self.transmission_complete_dir):
            self.logger.error('\'%s\' is not a directory, aborting.', self.transmission_complete_dir)
            exit(3)
        if not os.access(self.transmission_complete_dir, os.R_OK | os.X_OK):
            self.logger.error('Download directory \'%s\' is not readable, aborting.', self.transmission_complete_dir)
            exit(3)
        # Download dir
        dl_extra_list = []
        dl_dir_list = os.listdir(self.transmission_complete_dir)
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
        self.logger.info('Found %s files in the Transmission "complete" dir (over the %s total) that are not tracked '
                         'by Transmission anymore (%s torrents tracked, %s items in "complete" dir \'%s\').'
                         '\nYou may add the --execute flag to delete them.',
                         len(dl_extra_list), len(dl_dir_list), len(self.torrent_list), len(dl_dir_list),
                         self.transmission_complete_dir)

        if execute:
            self.logger.debug('Deleting files:')
            for f in dl_extra_list:
                file_path_to_delete = self.transmission_complete_dir + '/' + f
                self.logger.debug('Deleting %s', file_path_to_delete)
                try:
                    if os.path.isfile(file_path_to_delete):
                        Path(file_path_to_delete).unlink()
                    elif os.path.isdir(file_path_to_delete):
                        shutil.rmtree(file_path_to_delete)
                    print("File deleted successfully.")
                except FileNotFoundError:
                    print("File not found.")
                except PermissionError:
                    print("Permission denied. Unable to delete the file.")
                except Exception as e:
                    print("An error occurred:", e)

            # TODO Incomplete dir

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
        transmission_helper.cleanup(args.execute)
    # elif vars(args).get('storage_delta'):
    # TODO implement the directories override?
    elif args.storage_delta is not None:
        transmission_helper.storage_delta(args.execute)
    else:
        print(transmission_helper.parser.format_help())
        exit(0)


if __name__ == "__main__":
    main()
