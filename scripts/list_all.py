import time
import argparse
from pprint import pprint
from networktables import NetworkTables
from tj2_tools.networktables.backups import Backups


def main():
    parser = argparse.ArgumentParser("list_all")
    parser.add_argument("-a", "--address", default="10.0.88.2")
    parser.add_argument("-p", "--port", default=1735)
    args = parser.parse_args()

    address = args.address
    port = args.port

    NetworkTables.startClient((address, port))
    time.sleep(2.0)

    table = NetworkTables.getTable("")

    backup = Backups.get_full_table(table)
    pprint(backup)


if __name__ == "__main__":
    main()
