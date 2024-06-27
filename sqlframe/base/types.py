# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

import typing as t
from decimal import Decimal

from sqlframe.base.exceptions import RowError


class DataType:
    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: t.Any) -> bool:
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other: t.Any) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.simpleString()

    @classmethod
    def typeName(cls) -> str:
        return cls.__name__[:-4].lower()

    def simpleString(self) -> str:
        return self.typeName()

    def jsonValue(self) -> t.Union[str, t.Dict[str, t.Any]]:
        return str(self)


class DataTypeWithLength(DataType):
    def __init__(self, length: int):
        self.length = length

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.length})"

    def __str__(self) -> str:
        return f"{self.typeName()}({self.length})"


class StringType(DataType):
    pass


class CharType(DataTypeWithLength):
    pass


class VarcharType(DataTypeWithLength):
    pass


class BinaryType(DataType):
    pass


class BooleanType(DataType):
    pass


class DateType(DataType):
    pass


class TimestampType(DataType):
    pass


class TimestampNTZType(DataType):
    @classmethod
    def typeName(cls) -> str:
        return "timestamp_ntz"


class DecimalType(DataType):
    def __init__(self, precision: int = 10, scale: int = 0):
        self.precision = precision
        self.scale = scale

    def simpleString(self) -> str:
        return f"decimal({self.precision}, {self.scale})"

    def jsonValue(self) -> str:
        return f"decimal({self.precision}, {self.scale})"

    def __repr__(self) -> str:
        return f"DecimalType({self.precision}, {self.scale})"


class DoubleType(DataType):
    pass


class FloatType(DataType):
    pass


class ByteType(DataType):
    def __str__(self) -> str:
        return "tinyint"


class IntegerType(DataType):
    def __str__(self) -> str:
        return "int"


class LongType(DataType):
    def __str__(self) -> str:
        return "bigint"


class ShortType(DataType):
    def __str__(self) -> str:
        return "smallint"


class ArrayType(DataType):
    def __init__(self, elementType: DataType, containsNull: bool = True):
        self.elementType = elementType
        self.containsNull = containsNull

    def __repr__(self) -> str:
        return f"ArrayType({self.elementType, str(self.containsNull)}"

    def simpleString(self) -> str:
        return f"array<{self.elementType.simpleString()}>"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {
            "type": self.typeName(),
            "elementType": self.elementType.jsonValue(),
            "containsNull": self.containsNull,
        }


class MapType(DataType):
    def __init__(self, keyType: DataType, valueType: DataType, valueContainsNull: bool = True):
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def __repr__(self) -> str:
        return f"MapType({self.keyType}, {self.valueType}, {str(self.valueContainsNull)})"

    def simpleString(self) -> str:
        return f"map<{self.keyType.simpleString()}, {self.valueType.simpleString()}>"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {
            "type": self.typeName(),
            "keyType": self.keyType.jsonValue(),
            "valueType": self.valueType.jsonValue(),
            "valueContainsNull": self.valueContainsNull,
        }


class StructField(DataType):
    def __init__(
        self,
        name: str,
        dataType: DataType,
        nullable: bool = True,
        metadata: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def __repr__(self) -> str:
        return f"StructField('{self.name}', {self.dataType}, {str(self.nullable)})"

    def simpleString(self) -> str:
        return f"{self.name}:{self.dataType.simpleString()}"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {
            "name": self.name,
            "type": self.dataType.jsonValue(),
            "nullable": self.nullable,
            "metadata": self.metadata,
        }


class StructType(DataType):
    def __init__(self, fields: t.Optional[t.List[StructField]] = None):
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]

    def __iter__(self) -> t.Iterator[StructField]:
        return iter(self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def __repr__(self) -> str:
        return f"StructType({', '.join(str(field) for field in self)})"

    def simpleString(self) -> str:
        return f"struct<{', '.join(x.simpleString() for x in self)}>"

    def jsonValue(self) -> t.Dict[str, t.Any]:
        return {"type": self.typeName(), "fields": [x.jsonValue() for x in self]}

    def fieldNames(self) -> t.List[str]:
        return list(self.names)


def _create_row(
    fields: t.Union[Row, t.List[str]], values: t.Union[t.Tuple[t.Any, ...], t.List[t.Any]]
) -> Row:
    row = Row(*[float(x) if isinstance(x, Decimal) else x for x in values])
    row.__fields__ = fields
    return row


class Row(tuple):
    """
    A row in :class:`DataFrame`.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments.
    It is not allowed to omit a named argument to represent that the value is
    None or missing. This should be explicitly set to None in this case.

    .. versionchanged:: 3.0.0
        Rows created from named arguments no longer have
        field names sorted alphabetically and will be ordered in the position as
        entered.

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(name='Alice', age=11)
    >>> row['name'], row['age']
    ('Alice', 11)
    >>> row.name, row.age
    ('Alice', 11)
    >>> 'name' in row
    True
    >>> 'wrong_key' in row
    False

    Row also can be used to create another Row like class, then it
    could be used to create Row objects, such as

    >>> Person = Row("name", "age")
    >>> Person
    <Row('name', 'age')>
    >>> 'name' in Person
    True
    >>> 'wrong_key' in Person
    False
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)

    This form can also be used to create rows as tuple values, i.e. with unnamed
    fields.

    >>> row1 = Row("Alice", 11)
    >>> row2 = Row(name="Alice", age=11)
    >>> row1 == row2
    True
    """

    @t.overload
    def __new__(cls, *args: str) -> Row: ...

    @t.overload
    def __new__(cls, **kwargs: t.Any) -> Row: ...

    def __new__(cls, *args: t.Optional[str], **kwargs: t.Optional[t.Any]) -> Row:
        if args and kwargs:
            raise RowError("Cannot use both args and kwargs to create Row")
        if kwargs:
            # create row objects
            # psycopg2 returns Decimal type for numeric while PySpark returns float so we convert to float
            row = tuple.__new__(
                cls, [float(x) if isinstance(x, Decimal) else x for x in kwargs.values()]
            )
            row.__fields__ = list(kwargs.keys())
            return row
        else:
            # create row class or objects
            return tuple.__new__(cls, args)

    def asDict(self, recursive: bool = False) -> t.Dict[str, t.Any]:
        """
        Return as a dict

        Parameters
        ----------
        recursive : bool, optional
            turns the nested Rows to dict (default: False).

        Notes
        -----
        If a row contains duplicate field names, e.g., the rows of a join
        between two :class:`DataFrame` that both have the fields of same names,
        one of the duplicate fields will be selected by ``asDict``. ``__getitem__``
        will also return one of the duplicate fields, however returned value might
        be different to ``asDict``.

        Examples
        --------
        >>> from pyspark.sql import Row
        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(name='a', age=2)}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """
        if not hasattr(self, "__fields__"):
            raise RowError("Cannot convert a Row class into dict")

        if recursive:

            def conv(obj: t.Any) -> t.Any:
                if isinstance(obj, Row):
                    return obj.asDict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj

            return dict(zip(self.__fields__, (conv(o) for o in self)))
        else:
            return dict(zip(self.__fields__, self))

    def __contains__(self, item: t.Any) -> bool:
        if hasattr(self, "__fields__"):
            return item in self.__fields__
        else:
            return super(Row, self).__contains__(item)

    # let object acts like class
    def __call__(self, *args: t.Any) -> Row:
        """create new Row object"""
        if len(args) > len(self):
            raise RowError(
                "Cannot create Row with fields %s. Expected %d values but got %s instead"
                % (self, len(self), args)
            )

        return _create_row(self, args)

    def __getitem__(self, item: t.Any) -> t.Any:
        if isinstance(item, (int, slice)):
            return super(Row, self).__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return super(Row, self).__getitem__(idx)
        except IndexError:
            raise KeyError(item)
        except ValueError:
            raise RowError(item)

    def __getattr__(self, item: str) -> t.Any:
        if item.startswith("__"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return self[idx]
        except IndexError:
            raise AttributeError(item)
        except ValueError:
            raise AttributeError(item)

    def __setattr__(self, key: t.Any, value: t.Any) -> None:
        if key != "__fields__":
            raise RuntimeError("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(
        self,
    ) -> t.Union[str, t.Tuple[t.Any, ...]]:
        """Returns a tuple so Python knows how to pickle Row."""
        if hasattr(self, "__fields__"):
            return (_create_row, (self.__fields__, tuple(self)))
        else:
            return tuple.__reduce__(self)

    def __repr__(self) -> str:
        """Printable representation of Row used in Python REPL."""
        if hasattr(self, "__fields__"):
            return "Row(%s)" % ", ".join(
                "%s=%r" % (k, v) for k, v in zip(self.__fields__, tuple(self))
            )
        else:
            return "<Row(%s)>" % ", ".join(repr(field) for field in self)

    # SQLFrame Specific
    @property
    def _unique_field_names(self) -> t.List[str]:
        fields = []
        for i, field in enumerate(self.__fields__):
            if field in fields:
                field = field + "_" + str(i)
            fields.append(field)
        return fields
