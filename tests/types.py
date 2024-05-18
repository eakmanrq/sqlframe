import typing as t

if t.TYPE_CHECKING:
    EmployeeData = t.List[t.Tuple[int, str, str, int, int]]
    StoreData = t.List[t.Tuple[int, str, int, int]]
    DistrictData = t.List[t.Tuple[int, str]]
