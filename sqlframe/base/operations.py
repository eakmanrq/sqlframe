# This code is based on code from Apache Spark under the license found in the LICENSE file located in the 'sqlframe' folder.

from __future__ import annotations

from enum import IntEnum


class Operation(IntEnum):
    INIT = -1
    NO_OP = 0
    FROM = 1
    WHERE = 2
    GROUP_BY = 3
    HAVING = 4
    SELECT = 5
    ORDER_BY = 6
    LIMIT = 7
