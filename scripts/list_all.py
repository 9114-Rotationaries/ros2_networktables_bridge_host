import time
import argparse
from pprint import pprint
from networktables import (
    NetworkTables,
    NetworkTable,
    NetworkTablesInstance,
    NetworkTableEntry,
)
from typing import Dict, Union, ByteString, Callable, Tuple, List

ConvertedNtValue = Union[
    float, str, bool, bytes, List[float], List[str], List[bool], List[bytes]
]

ConvertedTable = Dict[str, Tuple[str, ConvertedNtValue]]

NT_TYPE_GET_MAPPING: Dict[
    ByteString, Callable[[NetworkTableEntry], ConvertedNtValue]
] = {
    NetworkTablesInstance.EntryTypes.BOOLEAN: lambda entry: entry.getBoolean(False),
    NetworkTablesInstance.EntryTypes.DOUBLE: lambda entry: entry.getDouble(0.0),
    NetworkTablesInstance.EntryTypes.STRING: lambda entry: entry.getString(""),
    NetworkTablesInstance.EntryTypes.RAW: lambda entry: entry.getRaw(b""),
    NetworkTablesInstance.EntryTypes.BOOLEAN_ARRAY: lambda entry: [
        bool(x) for x in entry.getBooleanArray([])
    ],
    NetworkTablesInstance.EntryTypes.DOUBLE_ARRAY: lambda entry: [
        float(x) for x in entry.getDoubleArray([])
    ],
    NetworkTablesInstance.EntryTypes.STRING_ARRAY: lambda entry: [
        str(x) for x in entry.getStringArray([])
    ],
}
NT_TYPE_NAME_MAPPING = {
    NetworkTablesInstance.EntryTypes.BOOLEAN: "bool",
    NetworkTablesInstance.EntryTypes.DOUBLE: "float",
    NetworkTablesInstance.EntryTypes.STRING: "str",
    NetworkTablesInstance.EntryTypes.RAW: "bytes",
    NetworkTablesInstance.EntryTypes.BOOLEAN_ARRAY: "List[bool]",
    NetworkTablesInstance.EntryTypes.DOUBLE_ARRAY: "List[float]",
    NetworkTablesInstance.EntryTypes.STRING_ARRAY: "List[str]",
}


def recurse_nt(data: ConvertedTable, current_path: str, table: NetworkTable):
    for entry_name in table.getKeys():
        entry = table.getEntry(entry_name)
        entry_type = entry.getType()
        get_fn = NT_TYPE_GET_MAPPING[entry_type]
        value = get_fn(table.getEntry(entry_name))
        data[current_path + "/" + entry_name] = (
            NT_TYPE_NAME_MAPPING[entry_type],
            value,
        )
    for key in table.getSubTables():
        if current_path == "/":
            next_path = current_path + key
        else:
            next_path = current_path + "/" + key
        if len(next_path.strip("/")) == 0:
            continue
        recurse_nt(data, next_path, table.getSubTable(key))


def get_full_table(root_table: NetworkTable) -> ConvertedTable:
    table: ConvertedTable = {}
    path = root_table.getPath()
    if path.endswith("/") and len(path) > 1:
        path = path[:-1]
    recurse_nt(table, path, root_table)
    return table


def main():
    parser = argparse.ArgumentParser("list_all")
    parser.add_argument("address")
    parser.add_argument("-p", "--port", default=1735)
    args = parser.parse_args()

    address = args.address
    port = args.port

    NetworkTables.startClient((address, port))
    time.sleep(2.0)

    table = NetworkTables.getTable("/")

    start_time = time.time()
    while len(table.getSubTables()) == 0 and len(table.getKeys()) == 0:
        time.sleep(0.1)
        if time.time() - start_time > 5.0:
            raise RuntimeError("Timed out waiting for NT entries")

    backup = get_full_table(table)
    pprint(backup)

    NetworkTables.stopClient()
    NetworkTables.shutdown()


if __name__ == "__main__":
    main()
