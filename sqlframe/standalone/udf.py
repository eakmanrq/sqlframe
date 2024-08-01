from __future__ import annotations

import typing as t

from sqlframe.base.udf import _BaseUDFRegistration

if t.TYPE_CHECKING:
    from sqlframe.standalone.session import StandaloneSession


class StandaloneUDFRegistration(_BaseUDFRegistration["StandaloneSession"]): ...
