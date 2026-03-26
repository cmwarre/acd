import os
import tempfile
import shutil
from abc import abstractmethod
from dataclasses import dataclass
from os import PathLike
from pathlib import Path

from acd.l5x.export_l5x import ExportL5x
from acd.zip.unzip import Unzip
from acd.zip.write_acd import write_acd

from acd.database.acd_database import AcdDatabase
from acd.l5x.elements import DumpCompsRecords, RSLogix5000Content


# Clean top-level API

def load_acd(path, temp_dir: str = None) -> RSLogix5000Content:
    """Load an ACD file into a Python object model.

    Args:
        path: Path to the .ACD file.
        temp_dir: Directory for SQLite and extracted files.  A temporary
            directory is created and cleaned up automatically if omitted.

    Returns:
        RSLogix5000Content with a fully populated controller object tree.
        The project also carries _raw_files / _file_order / _footer_unknown
        for use by save_acd().
    """
    cleanup = temp_dir is None
    if cleanup:
        temp_dir = tempfile.mkdtemp(prefix="acd_load_")
    try:
        exporter = ExportL5x(str(path), temp_dir)
        return exporter.project
    finally:
        if cleanup:
            shutil.rmtree(temp_dir, ignore_errors=True)


def save_acd(project: RSLogix5000Content, output_path) -> None:
    """Write a project object model back to an ACD file.

    The project must have been loaded via load_acd() or ExportL5x so that
    it carries _raw_files, _file_order, and _footer_unknown.

    Args:
        project: Project loaded by load_acd().
        output_path: Destination .ACD file path.
    """
    write_acd(
        files=project._raw_files,
        output_path=output_path,
        file_order=project._file_order,
        footer_unknown=project._footer_unknown,
    )


# Returned Project Structures


# Import Export Interfaces
class ImportProject:
    """ "Interface to import an PLC project"""

    @abstractmethod
    def import_project(self) -> RSLogix5000Content:
        # Import Project Interface
        pass


class ExportProject:
    """ "Interface to export an PLC project"""

    @abstractmethod
    def export_project(self, project: RSLogix5000Content):
        # Export Project Interface
        pass


# Concreate examples of importing and exporting projects
@dataclass
class ImportProjectFromFile(ImportProject):
    """Import a Controller from an ACD stored on file"""

    filename: PathLike

    def import_project(self) -> RSLogix5000Content:
        # Import Project Interface
        export = ExportL5x(self.filename)
        return export.project


@dataclass
class ExportProjectToFile(ExportProject):
    """Export a Controller to an ACD file"""

    filename: PathLike

    def export_project(self, project: RSLogix5000Content):
        # Concreate example of exporting a Project Object to an ACD file
        raise NotImplementedError


# Extracting/Compressing files from an ACD file Interfaces
class Extract:
    """Base class for all extract functions"""

    @abstractmethod
    def extract(self):
        # Interface for extracting database files
        pass


class Compress:
    """Base class for all compress functions"""

    @abstractmethod
    def compress(self):
        # Interface for extracting database files
        pass


# Concreate examples of extracting and compressing ACD files
@dataclass
class ExtractAcdDatabase(Extract):
    """Extract database files from a Logix ACD file"""

    filename: PathLike
    output_directory: PathLike

    def extract(self):
        # Implement the extraction of an ACD file
        unzip = Unzip(self.filename)
        unzip.write_files(self.output_directory)


@dataclass
class CompressAcdDatabase(Extract):
    """Compress database files to a Logix ACD file"""

    filename: PathLike
    output_directory: PathLike

    def compress(self):
        # Implement the compressing of an ACD file
        raise NotImplementedError


@dataclass
class ExtractAcdDatabaseRecordsToFiles(ExportProject):
    """Export all ACD databases to a raw database record tree"""

    filename: PathLike
    output_directory: PathLike

    def extract(self):
        # Implement the extraction of an ACD file
        database = AcdDatabase(self.filename, self.output_directory)
        database.extract_to_file()


@dataclass
class DumpCompsRecordsToFile(ExportProject):
    """
    Dump the Comps database to a folder. Each individual record can then be navigated and viewed.

    :param str filename: Filename of ACD file
    :param str output_directory: Location to store the records
    """

    filename: PathLike
    output_directory: PathLike

    def extract(self):
        export = ExportL5x(self.filename)
        with open(
            os.path.join(self.output_directory, "output.log"),
            "w",
        ) as log_file:
            DumpCompsRecords(export._cur, 0).dump(log_file=log_file)
