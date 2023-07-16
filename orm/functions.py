from enum import Enum, auto


class SqlFunction(Enum):
    NOW = auto()  # NOW()

    def convert_to_sql(self) -> str:
        return {
            SqlFunction.NOW: "NOW()",
        }[self]
