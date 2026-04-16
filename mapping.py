import struct
import re
import math
from exceptions import *

class Mapper:
    """Base class for all mappers. Handles byte-level serialization and deserialization."""
    def mapped_size(self) -> int:
        """Returns the number of bytes needed to store the mapped value."""
        pass

    def internal_map(self, value: str) -> int:
        """Defines the logic for mapping a string to its internal representation."""
        pass

    def from_bytes(self, value: bytes) -> int:
        """Deserializes a byte sequence into an integer."""
        return int.from_bytes(value, byteorder="big")

    def to_bytes(self, value: int) -> bytes:
        """Serializes an integer into a byte sequence of fixed size."""
        return value.to_bytes(self.mapped_size(), byteorder="big")

    def map_value(self, value: str) -> int:
        """Maps a string to an internal integer value."""
        try:
            mapped = self.internal_map(value)
        except Exception as e:
            raise MappingException(e)
        return mapped

    def unmap_value(self, value: int) -> str:
        """Converts an internal representation back to a string value."""
        pass


class FloatMapper(Mapper):
    """Maps float values into 4-byte float representation."""
    def mapped_size(self) -> int:
        """Returns 4 bytes for a standard float representation."""
        return 4

    def internal_map(self, value: str) -> float:
        """Parses a string into a float, raises exception if invalid."""
        try:
            return float(value)
        except Exception:
            raise InvalidConversionException(f"{value} cannot be converted to a float.")

    def to_bytes(self, value: float) -> bytes:
        """Serializes a float to 4 bytes."""
        return struct.pack(">f", value)

    def from_bytes(self, value: bytes) -> float:
        """Deserializes 4 bytes into a float."""
        return struct.unpack(">f", value)[0]

    def unmap_value(self, value: float) -> str:
        """Returns the float value as a string."""
        return str(value)


class CharMapper(Mapper):
    """Maps strings to a fixed-size ASCII representation padded with null bytes."""
    def __init__(self, size: int):
        """Initializes the mapper with a fixed size."""
        self.size = size

    def mapped_size(self) -> int:
        """Returns the fixed byte size reserved for the string."""
        return self.size

    def internal_map(self, value: str) -> str:
        """Validates the string fits within the fixed size."""
        if len(value) > self.size:
            raise InvalidMapException(f"{value} is longer than fixed size {self.size}")
        return value

    def to_bytes(self, value: str) -> bytes:
        """Encodes the string in ASCII and pads it to the required size."""
        return value.encode("ascii").ljust(self.mapped_size(), b"\x00")

    def from_bytes(self, value: bytes) -> str:
        """Decodes a byte string, stripping trailing null bytes."""
        return value.rstrip(b"\x00").decode("ascii")

    def unmap_value(self, value: str) -> str:
        """Returns the original string."""
        return value


class ShortMapper(Mapper):
    """Maps integers into 2-byte unsigned short representation."""
    def mapped_size(self) -> int:
        """Returns 2 bytes for a short."""
        return 2

    def internal_map(self, value: str) -> int:
        """Converts a string to an integer and validates short size."""
        try:
            mapped = int(value)
        except Exception:
            raise InvalidConversionException(f"{value} cannot be converted to a short.")

        if mapped > 2 ** 16 - 1:
            raise DataOverflowException(f"{value} is too large for a short.")

        return mapped

    def unmap_value(self, value: int) -> str:
        """Returns the integer value as a string."""
        return str(value)


class EnumMapper(Mapper):
    """Maps string values to unique integer indices based on a fixed list of allowed values."""
    def __init__(self, values: list[str]):
        """Creates a mapping from string values to integer indices."""
        self.map = {}
        self.values = values
        for i in range(len(values)):
            self.map[values[i]] = i

    def mapped_size(self) -> int:
        """Returns the number of bytes needed to store all distinct values."""
        return int(math.ceil(len(self.map).bit_length() / 8.0))

    def internal_map(self, value: str) -> int:
        """Maps a string to its corresponding index."""
        if value not in self.map:
            raise InvalidMapException(f"{value} is not present in the mappings for {self.__class__.__name__}")
        return self.map[value]

    def unmap_value(self, value: int) -> str:
        """Returns the original string from its index."""
        return self.values[value]


class TownMapper(EnumMapper):
    """EnumMapper for mapping town."""
    pass

class FlatTypeMapper(EnumMapper):
    """EnumMapper for mapping flat_type."""
    pass

class FlatModelMapper(EnumMapper):
    """EnumMapper for mapping flat_model."""
    pass

class StoreyRangeMapper(EnumMapper):
    """EnumMapper for mapping storey_range."""
    pass


class MonthMapper(Mapper):
    """Maps YYYY-MM formatted strings into a 2-byte representation as (year * 12 + (month - 1))."""
    def __init__(self):
        """Compiles a regex pattern for validating month strings."""
        self.pattern = re.compile(r'[0-9]{4}-[0-9]{2}')

    def mapped_size(self) -> int:
        """Returns 2 bytes to store month encoding."""
        return 2

    def internal_map(self, value: str) -> int:
        """Parses and validates YYYY-MM format and maps to numeric representation."""
        if not re.match(self.pattern, value):
            raise InvalidDateException(f"{value} is not in the format YYYY-MM.")

        try:
            year = int(value[:4])
        except Exception:
            raise InvalidDateException(f"{value[:4]} is not a valid year.")

        try:
            month = int(value[-2:])
            if not (0 < month <= 12):
                raise Exception()
        except Exception:
            raise InvalidDateException(f"{value[-2:]} is not a valid month")

        mapped = year * 12 + (month - 1)
        if mapped > 2 ** 16 - 1:
            raise DataOverflowException(f"{value} is too big to fit into 2 bytes.")

        return mapped

    def unmap_value(self, value: int) -> str:
        """Converts the internal numeric representation back to YYYY-MM format."""
        month = value % 12 + 1
        year = value // 12
        return str(year).zfill(4) + "-" + str(month).zfill(2)


class BlockMapper(Mapper):
    """Maps block identifiers like "123", "123A" into a 3-byte format."""
    def __init__(self):
        """Compiles a regex pattern for validating block identifiers."""
        self.pattern = re.compile(r"[0-9]+[A-Z]?")

    def mapped_size(self) -> int:
        """Returns 3 bytes: 2 for the number and 1 for the optional letter."""
        return 3

    def internal_map(self, value: str) -> int:
        """Parses and encodes a block number and optional letter into a compact integer."""
        if not re.match(self.pattern, value):
            raise InvalidBlockException(f"{value} should be a number followed by an optional uppercase letter.")

        result = 0
        if ord('A') <= ord(value[-1]) <= ord('Z'):
            result = ord(value[-1])
            value = value[:-1]

        result <<= 16
        block = int(value)

        if block > 2 ** 16 - 1:
            raise DataOverflowException(f"{block} too big to fit into 2 bytes.")

        result += int(value)
        return result

    def unmap_value(self, value: int) -> str:
        """Decodes the numeric-encoded block back into its original string format."""
        block = str(value & 0xFFFF)
        letter = value >> 16
        if letter != 0:
            block += chr(letter)

        return block
    

def get_query_params(matric_num):
    # a) Target Year: Last digit of matric matches last digit of YYYY [cite: 17]
    # Note: If last digit is 7, Year is 2017. 
    year_digit = int(matric_num[-2]) 
    target_year = 2010 + year_digit
    if target_year < 2015: target_year += 10 # Adjusts for the 2015-2024 range [cite: 11]

    # b) Commencing Month: Second last digit ("0" is Oct) [cite: 20]
    month_digit = int(matric_num[-3])
    start_month = 10 if month_digit == 0 else month_digit

    # c) Town List: Based on all digits in matric [cite: 21, 24]
    # Extract every unique digit from the matriculation number
    digits = set(re.findall(r'\d', matric_num))
    
    # Map digits using Table 1 from the manual [cite: 23]
    town_map = {
        '0': 'BEDOK', '1': 'BUKIT PANJANG', '2': 'CLEMENTI', '3': 'CHOA CHU KANG',
        '4': 'HOUGANG', '5': 'JURONG WEST', '6': 'PASIR RIS', '7': 'TAMPINES',
        '8': 'WOODLANDS', '9': 'YISHUN'
    }
    target_towns = [town_map[d] for d in digits if d in town_map]

    return target_year, start_month, target_towns