import os
import xml.dom.minidom
from abc import abstractmethod
from dataclasses import dataclass
from os import PathLike

from acd.l5x.export_l5x import ExportL5x
from acd.zip.unzip import Unzip

from acd.database.acd_database import AcdDatabase
from acd.l5x.elements import DumpCompsRecords, RSLogix5000Content


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


@dataclass
class ConvertAcdToL5x(Extract):
    """Convert an ACD file to an L5X XML file.

    Parses the ACD binary databases (Comps.Dat, SbRegion.Dat, Comments.Dat)
    and serialises the in-memory project model to an L5X-compatible XML file
    that can be imported back into Studio 5000 Logix Designer.

    The output captures controller tags, programs, routines (ladder rungs),
    data types (UDTs), add-on instructions (AOIs), and hardware modules.

    :param PathLike acd_filename: Path to the source .ACD file.
    :param PathLike l5x_filename: Path for the output .L5X file.
    :param bool pretty_print: Pretty-print the XML output (default True).
    """

    acd_filename: PathLike
    l5x_filename: PathLike
    pretty_print: bool = True

    def extract(self):
        project = ImportProjectFromFile(self.acd_filename).import_project()
        raw_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + project.to_xml()
        if self.pretty_print:
            try:
                dom = xml.dom.minidom.parseString(raw_xml.encode("utf-8"))
                output = dom.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")
                # minidom adds its own XML declaration; strip the duplicate header
                lines = output.splitlines()
                if lines and lines[0].startswith("<?xml"):
                    lines[0] = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                output = "\n".join(lines)
            except Exception:
                output = raw_xml
        else:
            output = raw_xml
        with open(self.l5x_filename, "w", encoding="utf-8") as f:
            f.write(output)
